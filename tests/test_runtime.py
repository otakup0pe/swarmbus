import asyncio
from unittest.mock import AsyncMock

import pytest

from swarmbus.message import AgentMessage
from swarmbus.runtime import ManagedMCPRuntime, _ManualAckAdapter


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
