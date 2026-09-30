import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from swarmbus.mcp_server import create_mcp_app
from swarmbus.runtime import ManagedMCPRuntime


class _FakePresenceMsg:
    def __init__(self, payload: dict):
        self.payload = json.dumps(payload).encode()


class _FakePresenceClient:
    """Fake aiomqtt client that replays a preset list of retained presence messages."""
    def __init__(self, retained: list[dict]):
        self._retained = retained
    async def __aenter__(self):
        return self
    async def __aexit__(self, *_):
        pass
    async def subscribe(self, *args, **kwargs):
        pass
    @property
    def messages(self):
        async def _gen():
            for p in self._retained:
                yield _FakePresenceMsg(p)
        return _gen()


@pytest.mark.asyncio
async def test_send_message_tool_calls_bus():
    with patch("swarmbus.mcp_server.AgentBus") as MockBus:
        instance = MockBus.return_value
        instance.send = AsyncMock()

        app = create_mcp_app(agent_id="sparrow", broker="localhost")
        send_fn = app._tool_fns["send_message"]

        await send_fn(
            to="wren",
            subject="hello",
            body="world",
            priority="high",
            reply_to="sparrow",
        )

        instance.send.assert_called_once_with(
            to="wren",
            subject="hello",
            body="world",
            content_type="text/plain",
            priority="high",
            reply_to="sparrow",
        )


@pytest.mark.asyncio
async def test_list_agents_compatibility_tool_remains_available():
    retained = [
        {"agent": "sparrow", "status": "online"},
        {"agent": "ghost", "status": "offline"},
    ]
    with patch(
        "swarmbus.bus.aiomqtt.Client",
        return_value=_FakePresenceClient(retained),
    ):
        app = create_mcp_app(agent_id="sparrow", broker="localhost")
        result = await app._tool_fns["list_agents"]()

    assert result == ["sparrow"]
    assert "agent_state" in app._tool_fns


@pytest.mark.asyncio
async def test_read_inbox_logs_broker_error(caplog):
    """Broker errors must log at ERROR, not silently return []."""
    import aiomqtt as _aiomqtt

    class _BadClient:
        async def __aenter__(self):
            raise _aiomqtt.MqttError("connection refused")
        async def __aexit__(self, *_):
            pass

    with patch("swarmbus.bus.aiomqtt.Client", return_value=_BadClient()):
        app = create_mcp_app(agent_id="sparrow", broker="localhost")
        with caplog.at_level("ERROR", logger="swarmbus.bus"):
            result = await app._tool_fns["read_inbox"]()
    assert result == []
    assert any("broker error" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_create_mcp_app_threads_persistent_to_bus():
    with patch("swarmbus.mcp_server.AgentBus") as MockBus:
        MockBus.return_value.send = AsyncMock()
        create_mcp_app(agent_id="sparrow", broker="localhost", persistent=True)
    assert MockBus.call_args.kwargs["persistent"] is True


@pytest.mark.asyncio
async def test_create_mcp_app_persistent_defaults_false():
    with patch("swarmbus.mcp_server.AgentBus") as MockBus:
        MockBus.return_value.send = AsyncMock()
        create_mcp_app(agent_id="sparrow", broker="localhost")
    assert MockBus.call_args.kwargs.get("persistent", False) is False


class _FakeManagedRuntime:
    def __init__(self):
        self.send_message = AsyncMock()
        self.read_inbox = AsyncMock(return_value=[])
        self.watch_inbox = AsyncMock(return_value=None)
        self.list_agents = AsyncMock(return_value=["loom"])
        self.list_states = AsyncMock(return_value=[{"agent_id": "loom"}])
        self.get_state = AsyncMock(return_value={"agent_id": "wren"})
        self.update_state = AsyncMock(return_value={"agent_id": "loom"})


@pytest.mark.asyncio
async def test_mcp_tools_use_injected_runtime():
    runtime = _FakeManagedRuntime()
    app = create_mcp_app(agent_id="loom", runtime=runtime)

    await app._tool_fns["send_message"](
        to="wren",
        subject="hello",
        body="world",
        priority="high",
        reply_to="loom",
    )
    await app._tool_fns["read_inbox"]()
    await app._tool_fns["watch_inbox"](timeout=4)
    assert await app._tool_fns["list_agents"]() == ["loom"]

    runtime.send_message.assert_awaited_once_with(
        to="wren",
        subject="hello",
        body="world",
        content_type="text/plain",
        priority="high",
        reply_to="loom",
    )
    runtime.read_inbox.assert_awaited_once_with()
    runtime.watch_inbox.assert_awaited_once_with(timeout=4)
    runtime.list_agents.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_agent_state_dispatches_parameterized_actions():
    runtime = _FakeManagedRuntime()
    app = create_mcp_app(agent_id="loom", runtime=runtime)
    tool = app._tool_fns["agent_state"]

    assert await tool(
        action="list",
        include_offline=True,
        lifecycle="persistent",
    ) == [{"agent_id": "loom"}]
    assert await tool(action="get", agent_id="wren") == {"agent_id": "wren"}
    assert await tool(
        action="update",
        status="working",
        working_set=["repo"],
        capabilities=["development.files.write"],
    ) == {"agent_id": "loom"}

    runtime.list_states.assert_awaited_once_with(
        include_offline=True,
        lifecycle="persistent",
    )
    runtime.get_state.assert_awaited_once_with("wren")
    runtime.update_state.assert_awaited_once_with(
        status="working",
        working_set=["repo"],
        capabilities=["development.files.write"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"action": "get"}, "requires agent_id"),
        (
            {"action": "list", "status": "nope"},
            "only accepts include_offline and lifecycle",
        ),
        ({"action": "update", "agent_id": "wren", "status": "x"}, "cannot target"),
        (
            {"action": "update"},
            "requires status, working_set, or capabilities",
        ),
    ],
)
async def test_agent_state_rejects_invalid_parameter_combinations(kwargs, message):
    app = create_mcp_app(agent_id="loom", runtime=_FakeManagedRuntime())

    with pytest.raises(ValueError, match=message):
        await app._tool_fns["agent_state"](**kwargs)


# ---------------------------------------------------------------------------
# topic_root -- namespacing the agent and registry trees
# ---------------------------------------------------------------------------


class _CapturingClient:
    """Fake aiomqtt client that records the topics published to it."""

    def __init__(self, published: list):
        self._published = published

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def publish(self, topic, payload, qos=0, retain=False):
        self._published.append(topic)


@pytest.mark.asyncio
async def test_create_mcp_app_default_topic_root_keeps_unrooted_topics():
    """No topic_root must reproduce the pre-rooting wire layout exactly --
    this literal is the backward-compatibility contract."""
    published: list = []
    with patch(
        "swarmbus.bus.aiomqtt.Client",
        return_value=_CapturingClient(published),
    ):
        app = create_mcp_app(agent_id="sparrow", broker="localhost")
        await app._tool_fns["send_message"](
            to="wren", subject="hello", body="world"
        )

    assert published == ["agents/wren/inbox"]


@pytest.mark.asyncio
async def test_create_mcp_app_topic_root_reaches_the_wire():
    published: list = []
    with patch(
        "swarmbus.bus.aiomqtt.Client",
        return_value=_CapturingClient(published),
    ):
        app = create_mcp_app(
            agent_id="sparrow", broker="localhost", topic_root="loom"
        )
        await app._tool_fns["send_message"](
            to="wren", subject="hello", body="world"
        )

    assert published == ["loom/agents/wren/inbox"]


@pytest.mark.asyncio
async def test_injected_runtime_keeps_its_own_topic_root():
    """Documented contract: topic_root is ignored when a runtime is
    injected, because that caller already chose the runtime's layout. The
    injected runtime here is rooted at "nest" while create_mcp_app is told
    "loom" -- the wire must show nest.
    """
    runtime = ManagedMCPRuntime(
        agent_id="sparrow",
        broker="localhost",
        state_path="unused.sqlite3",
        topic_root="nest",
    )
    runtime._client = MagicMock()
    runtime._client.publish = AsyncMock()

    app = create_mcp_app(agent_id="sparrow", runtime=runtime, topic_root="loom")
    await app._tool_fns["send_message"](to="wren", subject="hi", body="x")

    topic, _payload = runtime._client.publish.await_args.args
    assert topic == "nest/agents/wren/inbox"
