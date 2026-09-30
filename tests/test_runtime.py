import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from swarmbus.message import AgentMessage
from swarmbus.runtime import (
    ManagedMCPRuntime,
    TransportUnavailable,
    _ManualAckAdapter,
)


class _FakePaho:
    def __init__(self):
        self.enabled = None
        self.acks = []

    def manual_ack_set(self, enabled):
        self.enabled = enabled

    def ack(self, mid, qos):
        self.acks.append((mid, qos))
        return 0


class _FakeClient:
    def __init__(self):
        self._client = _FakePaho()
        self.publish = AsyncMock()


class _FakeStore:
    def __init__(self, events):
        self.events = events
        self.messages = []

    async def open(self):
        pass

    async def close(self):
        pass

    async def store(self, msg, *, source_topic):
        self.events.append("commit")
        self.messages.append(msg)
        return True

    async def drain(self, max_messages=10):
        items = self.messages[:max_messages]
        self.messages = self.messages[max_messages:]
        return [__import__("json").loads(msg.to_json()) for msg in items]


class _FakeMqttMessage:
    def __init__(self, msg):
        self.topic = "agents/loom/inbox"
        self.payload = msg.to_json().encode()
        self.mid = 42
        self.qos = 1


def _runtime(store):
    return ManagedMCPRuntime(
        agent_id="loom",
        broker="localhost",
        state_path="unused.sqlite3",
        store=store,
    )


def test_manual_ack_adapter_enables_paho_and_acks_message():
    client = _FakeClient()
    adapter = _ManualAckAdapter(client)

    adapter.enable()
    adapter.ack(_FakeMqttMessage(AgentMessage.create(
        from_="wren", to="loom", subject="hey", body="yo"
    )))

    assert client._client.enabled is True
    assert client._client.acks == [(42, 1)]


@pytest.mark.asyncio
async def test_inbox_commit_happens_before_qos1_ack():
    events = []
    store = _FakeStore(events)
    runtime = _runtime(store)

    class _Ack:
        def ack(self, message):
            events.append("ack")

    message = _FakeMqttMessage(AgentMessage.create(
        from_="wren", to="loom", subject="hey", body="yo"
    ))
    await runtime._handle_message(message, _Ack())

    assert events == ["commit", "ack"]


@pytest.mark.asyncio
async def test_store_failure_leaves_message_unacked():
    class _BrokenStore(_FakeStore):
        async def store(self, msg, *, source_topic):
            raise OSError("disk full")

    ack = AsyncMock()
    runtime = _runtime(_BrokenStore([]))
    message = _FakeMqttMessage(AgentMessage.create(
        from_="wren", to="loom", subject="hey", body="yo"
    ))

    with pytest.raises(OSError, match="disk full"):
        await runtime._handle_message(message, ack)

    ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_watch_inbox_wakes_after_committed_message():
    events = []
    store = _FakeStore(events)
    runtime = _runtime(store)
    runtime._client = _FakeClient()
    waiter = asyncio.create_task(runtime.watch_inbox(timeout=1))

    await asyncio.sleep(0)
    message = _FakeMqttMessage(AgentMessage.create(
        from_="wren", to="loom", subject="hey", body="yo"
    ))

    class _Ack:
        def ack(self, message):
            events.append("ack")

    await runtime._handle_message(message, _Ack())
    result = await waiter

    assert result["body"] == "yo"
    assert events == ["commit", "ack"]


@pytest.mark.asyncio
async def test_read_inbox_returns_local_messages_while_disconnected():
    message = AgentMessage.create(
        from_="wren", to="loom", subject="queued", body="from disk"
    )
    runtime = _runtime(_FakeStore([]))
    runtime.store.messages.append(message)

    result = await runtime.read_inbox()

    assert [item["id"] for item in result] == [message.id]


@pytest.mark.asyncio
async def test_empty_inbox_reports_disconnected_transport():
    runtime = _runtime(_FakeStore([]))

    with pytest.raises(TransportUnavailable, match="disconnected"):
        await runtime.read_inbox()
    with pytest.raises(TransportUnavailable, match="disconnected"):
        await runtime.watch_inbox(timeout=0.01)


@pytest.mark.asyncio
async def test_start_does_not_wait_for_broker_connection():
    runtime = _runtime(_FakeStore([]))
    connection_started = asyncio.Event()
    keep_running = asyncio.Event()

    async def _connection_loop():
        connection_started.set()
        await keep_running.wait()

    runtime._connection_loop = _connection_loop
    await asyncio.wait_for(runtime.start(), timeout=0.1)
    await asyncio.wait_for(connection_started.wait(), timeout=0.1)
    assert runtime._connection_task is not None
    await runtime.stop()


@pytest.mark.asyncio
async def test_background_connection_failure_is_logged_and_store_closes(caplog):
    class _TrackingStore(_FakeStore):
        closed = False

        async def close(self):
            self.closed = True

    store = _TrackingStore([])
    runtime = _runtime(store)

    async def _broken_connection_loop():
        raise RuntimeError("manual ACK unavailable")

    runtime._connection_loop = _broken_connection_loop
    with caplog.at_level(logging.ERROR, logger="swarmbus.runtime"):
        await runtime.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert "managed MQTT runtime stopped: manual ACK unavailable" in caplog.text
    with pytest.raises(RuntimeError, match="manual ACK unavailable"):
        await runtime.stop()
    assert store.closed is True


@pytest.mark.asyncio
async def test_stop_closes_store_when_offline_publish_fails():
    class _TrackingStore(_FakeStore):
        closed = False

        async def close(self):
            self.closed = True

    store = _TrackingStore([])
    runtime = _runtime(store)
    runtime._client = _FakeClient()
    runtime._connection_task = asyncio.create_task(asyncio.sleep(60))
    runtime._publish_presence = AsyncMock(side_effect=RuntimeError("link lost"))

    await runtime.stop()

    assert store.closed is True
    assert runtime._client is None


@pytest.mark.asyncio
async def test_send_message_preserves_envelope_metadata():
    runtime = _runtime(_FakeStore([]))
    runtime._client = _FakeClient()

    await runtime.send_message(
        to="wren",
        subject="reply requested",
        body="ping",
        priority="high",
        reply_to="loom",
    )

    topic, payload = runtime._client.publish.await_args.args
    message = AgentMessage.from_json(payload)
    assert topic == "agents/wren/inbox"
    assert message.priority == "high"
    assert message.reply_to == "loom"


@pytest.mark.asyncio
async def test_heartbeat_loop_survives_publish_failure():
    runtime = _runtime(_FakeStore([]))
    runtime._client = _FakeClient()
    runtime.heartbeat_seconds = 0.001

    calls = 0
    recovered = asyncio.Event()

    async def _flaky_publish():
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise RuntimeError("swarmbus MQTT runtime is not connected")
        recovered.set()

    runtime._publish_registry = _flaky_publish
    task = asyncio.create_task(runtime._heartbeat_loop())
    try:
        # The first two publishes raise; a resilient loop keeps going and
        # eventually lands a successful publish instead of dying silently.
        await asyncio.wait_for(recovered.wait(), timeout=1)
        assert not task.done()
        assert calls >= 3
    finally:
        runtime._stopping.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_update_state_preserves_omitted_fields_and_clears_explicit_values():
    runtime = _runtime(_FakeStore([]))
    runtime._client = _FakeClient()
    await runtime._publish_presence("online")

    first = await runtime.update_state(
        status="working",
        working_set=[" repo ", "broker", "repo"],
    )
    second = await runtime.update_state(status=None, working_set=[])
    third = await runtime.update_state(status="", working_set=None)

    assert first["status"] == "working"
    assert first["working_set"] == ["repo", "broker"]
    assert second["status"] == "working"
    assert second["working_set"] == []
    assert third["status"] == ""


@pytest.mark.asyncio
async def test_declared_capabilities_union_with_runtime_guarantees():
    runtime = _runtime(_FakeStore([]))
    runtime._client = _FakeClient()
    await runtime._publish_presence("online")

    first = await runtime.update_state(
        status=None,
        working_set=None,
        capabilities=[
            " development.files.write ",
            "agent-state",
            "yopo.facet.yopo",
        ],
    )
    second = await runtime.update_state(
        status="still working",
        working_set=None,
    )

    assert first["capabilities"] == [
        "messaging",
        "durable-inbox",
        "agent-state",
        "development.files.write",
        "yopo.facet.yopo",
    ]
    assert second["capabilities"] == first["capabilities"]


def test_transient_runtime_uses_explicit_mqtt_client_id():
    runtime = ManagedMCPRuntime(
        agent_id="loom-transient-session-deadbeef",
        broker="localhost",
        lifecycle="transient",
        client_id="session-deadbeef",
        state_path="unused.sqlite3",
        store=_FakeStore([]),
    )

    kwargs = runtime._client_kwargs()

    assert kwargs["identifier"] == "session-deadbeef"
    assert "clean_session" not in kwargs


def test_persistent_runtime_uses_explicit_stable_client_id():
    runtime = ManagedMCPRuntime(
        agent_id="named-facet",
        broker="localhost",
        lifecycle="persistent",
        persistent=True,
        client_id="swarmbus-named-facet",
        state_path="unused.sqlite3",
        store=_FakeStore([]),
    )

    kwargs = runtime._client_kwargs()

    assert kwargs["identifier"] == "swarmbus-named-facet"
    assert kwargs["clean_session"] is False


@pytest.mark.asyncio
async def test_clean_transient_shutdown_removes_retained_state():
    runtime = ManagedMCPRuntime(
        agent_id="loom-transient-session-deadbeef",
        broker="localhost",
        lifecycle="transient",
        state_path="unused.sqlite3",
        store=_FakeStore([]),
    )
    client = _FakeClient()
    runtime._client = client
    runtime._connection_task = asyncio.create_task(asyncio.sleep(60))
    await runtime._publish_presence("online")
    await runtime._publish_registry()

    await runtime.stop()

    tombstones = [
        call.args[:2] for call in client.publish.await_args_list
        if len(call.args) >= 2 and call.args[1] == b""
    ]
    assert tombstones == [
        ("swarmbus/registry/loom-transient-session-deadbeef", b""),
        ("agents/loom-transient-session-deadbeef/presence", b""),
    ]
    assert runtime.registry.list_states(include_offline=True) == []


# ---------------------------------------------------------------------------
# topic_root -- namespacing the agent and registry trees
# ---------------------------------------------------------------------------


def _rooted_runtime(store, *, agent_id="wren", root="loom"):
    return ManagedMCPRuntime(
        agent_id=agent_id,
        broker="localhost",
        state_path="unused.sqlite3",
        store=store,
        topic_root=root,
    )


class _FakeInboundMessage:
    """Inbound MQTT message on an arbitrary topic.

    _FakeMqttMessage above is pinned to the inbox topic; registry and
    presence dispatch need the topic to vary.
    """

    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload
        self.mid = 7
        self.qos = 1


class _RecordingAck:
    def __init__(self):
        self.acked = []

    def ack(self, message):
        self.acked.append(str(message.topic))


def _published_topics(client):
    return [call.args[0] for call in client.publish.await_args_list]


@pytest.mark.asyncio
async def test_default_runtime_publishes_historical_wire_topics():
    """No topic_root must reproduce the pre-rooting wire layout exactly.

    The literals below ARE the backward-compatibility contract for every
    deployment that never sets a root, so they are spelled out rather than
    derived from the map under test.
    """
    runtime = _runtime(_FakeStore([]))
    runtime._client = _FakeClient()

    await runtime._publish_presence("online")
    await runtime._publish_registry()
    await runtime.send_message(to="sparrow", subject="hi", body="x")
    await runtime.send_message(to="broadcast", subject="all", body="x")

    assert _published_topics(runtime._client) == [
        "agents/loom/presence",
        "swarmbus/registry/loom",
        "agents/sparrow/inbox",
        "agents/broadcast",
    ]
    assert str(runtime._client_kwargs()["will"].topic) == "agents/loom/presence"


@pytest.mark.asyncio
async def test_rooted_runtime_publishes_under_root():
    runtime = _rooted_runtime(_FakeStore([]))
    runtime._client = _FakeClient()

    await runtime._publish_presence("online")
    await runtime._publish_registry()
    await runtime.send_message(to="sparrow", subject="hi", body="x")

    assert _published_topics(runtime._client) == [
        "loom/agents/wren/presence",
        "loom/swarmbus/registry/wren",
        "loom/agents/sparrow/inbox",
    ]
    assert (
        str(runtime._client_kwargs()["will"].topic)
        == "loom/agents/wren/presence"
    )


@pytest.mark.asyncio
async def test_rooted_runtime_broadcast_is_never_rooted():
    """WHY this asymmetry exists: broadcast is bus-wide by design. Agents
    living under different topic roots must still receive each other's
    fan-out, so the broadcast topic deliberately ignores topic_root.
    Rooting it would silo every namespace and is exactly the "consistency
    fix" a future refactor will be tempted to make. Do not make it.
    """
    runtime = _rooted_runtime(_FakeStore([]))
    runtime._client = _FakeClient()

    await runtime.send_message(to="broadcast", subject="all hands", body="x")

    topic, _payload = runtime._client.publish.await_args.args
    assert topic == "agents/broadcast"


@pytest.mark.asyncio
async def test_rooted_runtime_registry_cache_parses_rooted_topics():
    """The RegistryCache the runtime builds must share the runtime's rooted
    map. Fed straight from what a rooted peer actually published, an
    observer with a default (unrooted) cache would raise ValueError while
    parsing `loom/swarmbus/registry/wren` instead of tracking the agent.
    """
    publisher = _rooted_runtime(_FakeStore([]))
    publisher._client = _FakeClient()
    await publisher._publish_presence("online")
    await publisher._publish_registry()
    wire = [
        call.args[:2] for call in publisher._client.publish.await_args_list
    ]

    observer = _rooted_runtime(_FakeStore([]), agent_id="sparrow")
    ack = _RecordingAck()
    for topic, payload in wire:
        await observer._handle_message(_FakeInboundMessage(topic, payload), ack)

    assert ack.acked == [
        "loom/agents/wren/presence",
        "loom/swarmbus/registry/wren",
    ]
    assert await observer.list_agents() == ["wren"]
    assert (await observer.get_state("wren"))["online"] is True
