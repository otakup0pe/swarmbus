"""Direct tests for AgentBus.read_inbox / watch_inbox / list_agents.

Before the refactor these methods only lived on the MCP server and were
tested indirectly. Now that they're first-class on AgentBus, cover the
timeout and malformed-envelope branches directly.
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import patch

import aiomqtt

from swarmbus.bus import AgentBus
from swarmbus.message import AgentMessage


class _FakeMsg:
    def __init__(self, payload: bytes):
        self.payload = payload


class _FakeClient:
    """Replays a preset list of payloads, then hangs (caller relies on timeout)."""
    def __init__(self, payloads: list[bytes]):
        self._payloads = payloads

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def subscribe(self, *_args, **_kwargs):
        pass

    @property
    def messages(self):
        async def _gen():
            for p in self._payloads:
                yield _FakeMsg(p)
        return _gen()


class _BadClient:
    async def __aenter__(self):
        raise aiomqtt.MqttError("connection refused")

    async def __aexit__(self, *_):
        pass


class _FakeTopicMsg:
    """list_states branches on TOPIC, unlike the payload-only methods."""
    def __init__(self, topic: str, payload: bytes):
        self.topic = topic
        self.payload = payload


class _FakeTopicClient:
    def __init__(self, pairs: list[tuple[str, bytes]]):
        self._pairs = pairs

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def subscribe(self, *_args, **_kwargs):
        pass

    @property
    def messages(self):
        async def _gen():
            for topic, payload in self._pairs:
                yield _FakeTopicMsg(topic, payload)
        return _gen()


def _registry_payload(agent_id: str, **overrides) -> bytes:
    # MUST be current. `RegistryCache._is_online` treats a registry record
    # older than `stale_after_seconds` (default 180) as stale, so a
    # hardcoded timestamp makes every agent read offline the moment the
    # fixture ages past three minutes -- a test that passes on the day it is
    # written and silently inverts later.
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    data = {
        "schema_version": 1,
        "agent_id": agent_id,
        "status": "",
        "working_set": [],
        "capabilities": ["messaging"],
        "durability": "durable",
        "started_at": now,
        "last_seen": now,
    }
    data.update(overrides)
    return json.dumps(data).encode()


def _presence_payload(agent_id: str, state: str) -> bytes:
    return json.dumps({
        "schema_version": 1,
        "agent_id": agent_id,
        "state": state,
    }).encode()


def _envelope(**overrides) -> bytes:
    """Build a valid AgentMessage JSON payload."""
    msg = AgentMessage.create(
        from_=overrides.get("from_", "sender"),
        to=overrides.get("to", "me"),
        subject=overrides.get("subject", "hi"),
        body=overrides.get("body", "hello"),
    )
    return msg.to_json().encode()


# --------------------------------------------------------------------------
# read_inbox
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_inbox_returns_valid_envelopes():
    payloads = [_envelope(subject="one"), _envelope(subject="two")]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus(agent_id="me")
        result = await bus.read_inbox(drain_timeout=0.1)
    assert len(result) == 2
    assert result[0]["subject"] == "one"
    assert result[1]["subject"] == "two"


@pytest.mark.asyncio
async def test_read_inbox_respects_max_messages():
    payloads = [_envelope(subject=f"m{i}") for i in range(5)]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus(agent_id="me")
        result = await bus.read_inbox(max_messages=3, drain_timeout=0.2)
    assert len(result) == 3


@pytest.mark.asyncio
async def test_read_inbox_skips_malformed_envelopes():
    payloads = [b"not even json", _envelope(subject="good"), b'{"partial": 1}']
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus(agent_id="me")
        result = await bus.read_inbox(drain_timeout=0.1)
    assert len(result) == 1
    assert result[0]["subject"] == "good"


@pytest.mark.asyncio
async def test_read_inbox_broker_error_raises():
    """CLI contract depends on this: MqttError must propagate so the CLI
    layer can print a clean error and exit 2 (instead of confusing broker-down
    with empty-inbox). MCP callers catch it at the tool boundary."""
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_BadClient()):
        bus = AgentBus(agent_id="me")
        with pytest.raises(aiomqtt.MqttError):
            await bus.read_inbox(drain_timeout=0.1)


@pytest.mark.asyncio
async def test_read_inbox_timeout_returns_what_it_has():
    payloads: list[bytes] = []  # nothing to yield → hits timeout
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus(agent_id="me")
        result = await bus.read_inbox(drain_timeout=0.05)
    assert result == []


# --------------------------------------------------------------------------
# watch_inbox
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watch_inbox_returns_first_valid_envelope():
    payloads = [_envelope(subject="first"), _envelope(subject="second")]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus(agent_id="me")
        result = await bus.watch_inbox(timeout=0.2)
    assert result is not None
    assert result["subject"] == "first"


@pytest.mark.asyncio
async def test_watch_inbox_skips_malformed_then_returns_good():
    payloads = [b"garbage", _envelope(subject="good")]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus(agent_id="me")
        result = await bus.watch_inbox(timeout=0.2)
    assert result is not None
    assert result["subject"] == "good"


@pytest.mark.asyncio
async def test_watch_inbox_timeout_returns_none():
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient([])):
        bus = AgentBus(agent_id="me")
        result = await bus.watch_inbox(timeout=0.05)
    assert result is None


@pytest.mark.asyncio
async def test_watch_inbox_broker_error_raises():
    """Same contract as read_inbox: MqttError propagates."""
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_BadClient()):
        bus = AgentBus(agent_id="me")
        with pytest.raises(aiomqtt.MqttError):
            await bus.watch_inbox(timeout=0.1)


@pytest.mark.asyncio
async def test_list_agents_broker_error_raises():
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_BadClient()):
        bus = AgentBus.probe()
        with pytest.raises(aiomqtt.MqttError):
            await bus.list_agents(collect_window=0.1)


# --------------------------------------------------------------------------
# list_agents
#
# These were MCP-tool-level tests in test_mcp_server.py until the
# `list_agents` MCP tool was removed 2026-08-25. The BUS method survives --
# it still backs `swarmbus list` -- so the coverage moved down a layer
# rather than being deleted with the tool.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_agents_filters_offline():
    payloads = [
        json.dumps({"agent": "sparrow", "status": "online"}).encode(),
        json.dumps({"agent": "wren", "status": "online"}).encode(),
        json.dumps({"agent": "ghost", "status": "offline"}).encode(),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus.probe()
        result = await bus.list_agents(collect_window=0.1)
    assert result == ["sparrow", "wren"]


@pytest.mark.asyncio
async def test_list_agents_latest_status_wins():
    """Multiple retained presence messages for one agent: the last one wins."""
    payloads = [
        json.dumps({"agent": "wren", "status": "online"}).encode(),
        json.dumps({"agent": "wren", "status": "offline"}).encode(),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus.probe()
        result = await bus.list_agents(collect_window=0.1)
    assert result == []


@pytest.mark.asyncio
async def test_list_agents_skips_malformed_payloads():
    """One unparseable retained payload must not blank the whole view.

    An empty result is indistinguishable from "no agents exist", which is
    the reading that caused two separate misdiagnoses. Skip the bad record
    and keep the good ones.
    """
    payloads = [
        b"not json at all",
        json.dumps({"agent": "sparrow", "status": "online"}).encode(),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeClient(payloads)):
        bus = AgentBus.probe()
        result = await bus.list_agents(collect_window=0.1)
    assert result == ["sparrow"]


# --------------------------------------------------------------------------
# list_states -- the replacement for the removed list_agents MCP tool.
#
# The distinction these cover is the whole point of the method: an agent that
# is REGISTERED BUT OFFLINE is not an absent agent. Reading it as absent is
# the mistake that got made twice.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_states_marks_stale_registry_record_offline():
    """A registry record older than stale_after_seconds is NOT online.

    Presence says "online" but the heartbeat has gone quiet -- that is a
    dead agent that never published its offline record, and treating it as
    live is how a deadhand misses a failure. Pinned explicitly because a
    hardcoded fixture timestamp made four other tests here fail this way by
    accident.
    """
    from datetime import datetime, timedelta, timezone
    old = (datetime.now(timezone.utc) - timedelta(hours=9)).isoformat()
    pairs = [
        ("swarmbus/registry/sparrow", _registry_payload("sparrow", last_seen=old)),
        ("agents/sparrow/presence", _presence_payload("sparrow", "online")),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeTopicClient(pairs)):
        bus = AgentBus.probe()
        default = await bus.list_states(collect_window=0.1)
        with patch("swarmbus.bus.aiomqtt.Client",
                   return_value=_FakeTopicClient(pairs)):
            widened = await bus.list_states(include_offline=True,
                                            collect_window=0.1)
    assert default == []
    assert {s["agent_id"] for s in widened} == {"sparrow"}
    assert widened[0]["online"] is False


@pytest.mark.asyncio
async def test_list_states_excludes_offline_by_default():
    pairs = [
        ("swarmbus/registry/sparrow", _registry_payload("sparrow")),
        ("agents/sparrow/presence", _presence_payload("sparrow", "online")),
        ("swarmbus/registry/ghost", _registry_payload("ghost")),
        ("agents/ghost/presence", _presence_payload("ghost", "offline")),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeTopicClient(pairs)):
        bus = AgentBus.probe()
        result = await bus.list_states(collect_window=0.1)
    assert {s["agent_id"] for s in result} == {"sparrow"}


@pytest.mark.asyncio
async def test_list_states_includes_offline_when_asked():
    """The parameter the removed tool accepted and ignored must actually work."""
    pairs = [
        ("swarmbus/registry/sparrow", _registry_payload("sparrow")),
        ("agents/sparrow/presence", _presence_payload("sparrow", "online")),
        ("swarmbus/registry/ghost", _registry_payload("ghost")),
        ("agents/ghost/presence", _presence_payload("ghost", "offline")),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeTopicClient(pairs)):
        bus = AgentBus.probe()
        result = await bus.list_states(include_offline=True, collect_window=0.1)
    by_id = {s["agent_id"]: s for s in result}
    assert set(by_id) == {"sparrow", "ghost"}
    assert by_id["sparrow"]["online"] is True
    assert by_id["ghost"]["online"] is False


@pytest.mark.asyncio
async def test_list_states_carries_status_and_working_set():
    pairs = [
        ("swarmbus/registry/sparrow",
         _registry_payload("sparrow", status="building", working_set=["a.py"])),
        ("agents/sparrow/presence", _presence_payload("sparrow", "online")),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeTopicClient(pairs)):
        bus = AgentBus.probe()
        result = await bus.list_states(collect_window=0.1)
    assert result[0]["status"] == "building"
    assert result[0]["working_set"] == ["a.py"]


@pytest.mark.asyncio
async def test_list_states_skips_malformed_and_keeps_the_rest():
    """One bad retained payload must not blank the view.

    An empty list reads as "nobody is there", which is exactly the
    misreading this method was added to prevent.
    """
    pairs = [
        ("swarmbus/registry/broken", b"not json at all"),
        ("agents/alsobroken/presence", b"{"),
        ("swarmbus/registry/sparrow", _registry_payload("sparrow")),
        ("agents/sparrow/presence", _presence_payload("sparrow", "online")),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeTopicClient(pairs)):
        bus = AgentBus.probe()
        result = await bus.list_states(collect_window=0.1)
    assert {s["agent_id"] for s in result} == {"sparrow"}


@pytest.mark.asyncio
async def test_list_states_rejects_agent_id_topic_mismatch():
    """A record whose agent_id disagrees with its topic is discarded.

    Guards against one agent publishing a registry record that claims to be
    another -- the topic is the broker-authenticated fact, the body is not.
    """
    pairs = [
        ("swarmbus/registry/sparrow", _registry_payload("impostor")),
        ("agents/sparrow/presence", _presence_payload("sparrow", "online")),
    ]
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakeTopicClient(pairs)):
        bus = AgentBus.probe()
        result = await bus.list_states(include_offline=True, collect_window=0.1)
    assert "impostor" not in {s["agent_id"] for s in result}


@pytest.mark.asyncio
async def test_list_states_broker_error_raises():
    """Same contract as the other bus methods: MqttError propagates.

    It must NOT degrade to an empty list -- that would be indistinguishable
    from "no agents registered".
    """
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_BadClient()):
        bus = AgentBus.probe()
        with pytest.raises(aiomqtt.MqttError):
            await bus.list_states(collect_window=0.1)


@pytest.mark.asyncio
async def test_probe_bypasses_agent_id_validation():
    """AgentBus.probe() must not raise even though `_probe` starts with _."""
    bus = AgentBus.probe(broker="localhost")
    assert bus.agent_id == "_probe"
    assert bus.broker == "localhost"


# --------------------------------------------------------------------------
# outbox logging on send
# --------------------------------------------------------------------------


class _NullPublishClient:
    """aiomqtt stand-in that accepts publish() and is a no-op otherwise."""
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def publish(self, *args, **kwargs):
        pass


@pytest.mark.asyncio
async def test_send_appends_to_outbox(tmp_path):
    outbox = tmp_path / "outbox.md"
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_NullPublishClient()):
        bus = AgentBus(agent_id="sparrow")
        await bus.send(
            to="wren",
            subject="hello",
            body="hi wren",
            outbox_path=str(outbox),
        )
    text = outbox.read_text()
    assert "To: wren" in text
    assert "hello" in text
    assert "hi wren" in text


@pytest.mark.asyncio
async def test_send_outbox_creates_parent_dirs(tmp_path):
    outbox = tmp_path / "nested" / "sparrow-outbox.md"
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_NullPublishClient()):
        bus = AgentBus(agent_id="sparrow")
        await bus.send(to="wren", subject="x", body="y", outbox_path=str(outbox))
    assert outbox.exists()


@pytest.mark.asyncio
async def test_send_outbox_disabled_when_unset(tmp_path):
    """No outbox_path → no file written; confirms the append is opt-in."""
    outbox = tmp_path / "should_not_exist.md"
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_NullPublishClient()):
        bus = AgentBus(agent_id="sparrow")
        await bus.send(to="wren", subject="x", body="y")
    assert not outbox.exists()


@pytest.mark.asyncio
async def test_listen_persistent_passes_stable_identifier_and_clean_session():
    """persistent=True → stable client-id + clean_session=False on the MQTT client."""
    captured_kwargs = {}

    class _ClientStub:
        def __init__(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            pass
        async def publish(self, *_args, **_kwargs):
            pass
        async def subscribe(self, *_args, **_kwargs):
            pass
        @property
        def messages(self):
            async def _empty():
                if False:
                    yield  # make generator
            return _empty()

    with patch("swarmbus.bus.aiomqtt.Client", _ClientStub):
        bus = AgentBus(agent_id="sparrow", persistent=True)
        await bus.listen()
    assert captured_kwargs.get("identifier") == "swarmbus-sparrow"
    assert captured_kwargs.get("clean_session") is False


@pytest.mark.asyncio
async def test_listen_non_persistent_omits_identifier():
    captured_kwargs = {}

    class _ClientStub:
        def __init__(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            pass
        async def publish(self, *_args, **_kwargs):
            pass
        async def subscribe(self, *_args, **_kwargs):
            pass
        @property
        def messages(self):
            async def _empty():
                if False:
                    yield
            return _empty()

    with patch("swarmbus.bus.aiomqtt.Client", _ClientStub):
        bus = AgentBus(agent_id="sparrow", persistent=False)
        await bus.listen()
    assert "identifier" not in captured_kwargs
    assert "clean_session" not in captured_kwargs


@pytest.mark.asyncio
async def test_send_outbox_agent_id_template_substitutes(tmp_path):
    """`{agent_id}` in outbox_path → replaced with the bus's agent_id."""
    template = str(tmp_path / "{agent_id}-outbox.md")
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_NullPublishClient()):
        bus_s = AgentBus(agent_id="sparrow")
        await bus_s.send(to="wren", subject="s", body="b1", outbox_path=template)
        bus_w = AgentBus(agent_id="wren")
        await bus_w.send(to="sparrow", subject="w", body="b2", outbox_path=template)
    assert (tmp_path / "sparrow-outbox.md").read_text().startswith("\n## ")
    assert (tmp_path / "wren-outbox.md").read_text().startswith("\n## ")
    assert "b1" in (tmp_path / "sparrow-outbox.md").read_text()
    assert "b2" in (tmp_path / "wren-outbox.md").read_text()
    assert "b2" not in (tmp_path / "sparrow-outbox.md").read_text()


# --------------------------------------------------------------------------
# read_inbox / watch_inbox persistent session kwargs
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_inbox_persistent_passes_identifier_and_clean_session():
    """persistent=True → stable client-id + clean_session=False on read_inbox."""
    captured_kwargs = {}
    payloads = [_envelope(subject="queued")]

    class _CapturingClient(_FakeClient):
        def __init__(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
            super().__init__(payloads)

    with patch("swarmbus.bus.aiomqtt.Client", _CapturingClient):
        bus = AgentBus(agent_id="sparrow", persistent=True)
        result = await bus.read_inbox(drain_timeout=0.1)
    assert captured_kwargs.get("identifier") == "swarmbus-sparrow"
    assert captured_kwargs.get("clean_session") is False
    assert len(result) == 1


@pytest.mark.asyncio
async def test_read_inbox_non_persistent_omits_identifier():
    """persistent=False (default) → no identifier or clean_session set."""
    captured_kwargs = {}

    class _CapturingClient(_FakeClient):
        def __init__(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
            super().__init__([])

    with patch("swarmbus.bus.aiomqtt.Client", _CapturingClient):
        bus = AgentBus(agent_id="sparrow", persistent=False)
        await bus.read_inbox(drain_timeout=0.1)
    assert "identifier" not in captured_kwargs
    assert "clean_session" not in captured_kwargs


@pytest.mark.asyncio
async def test_watch_inbox_persistent_passes_identifier_and_clean_session():
    """persistent=True → stable client-id + clean_session=False on watch_inbox."""
    captured_kwargs = {}
    payloads = [_envelope(subject="live")]

    class _CapturingClient(_FakeClient):
        def __init__(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
            super().__init__(payloads)

    with patch("swarmbus.bus.aiomqtt.Client", _CapturingClient):
        bus = AgentBus(agent_id="sparrow", persistent=True)
        result = await bus.watch_inbox(timeout=0.1)
    assert captured_kwargs.get("identifier") == "swarmbus-sparrow"
    assert captured_kwargs.get("clean_session") is False
    assert result is not None


@pytest.mark.asyncio
async def test_watch_inbox_non_persistent_omits_identifier():
    """persistent=False (default) → no identifier or clean_session set."""
    captured_kwargs = {}

    class _CapturingClient(_FakeClient):
        def __init__(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
            super().__init__([])

    with patch("swarmbus.bus.aiomqtt.Client", _CapturingClient):
        bus = AgentBus(agent_id="sparrow", persistent=False)
        await bus.watch_inbox(timeout=0.1)
    assert "identifier" not in captured_kwargs
    assert "clean_session" not in captured_kwargs
