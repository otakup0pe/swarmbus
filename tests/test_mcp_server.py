import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from swarmbus.mcp_server import create_mcp_app


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


# The `list_agents` MCP tool was removed 2026-08-25; `agent_state` is the
# single registry surface. Its bus-level behaviour (online filtering,
# latest-status-wins, malformed-payload skipping) still backs `swarmbus list`
# and is covered directly in tests/test_bus_methods.py.


@pytest.mark.asyncio
async def test_list_agents_tool_is_gone():
    """Regression guard: the tool must not come back by accident.

    It was removed because it accepted `include_offline` and SILENTLY ignored
    it, returning an online-only subset with no error -- which misled callers
    into "this agent was never registered" twice, six days apart.
    """
    with patch("swarmbus.bus.aiomqtt.Client", return_value=_FakePresenceClient([])):
        app = create_mcp_app(agent_id="sparrow", broker="localhost")
    assert "list_agents" not in app._tool_fns
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


@pytest.mark.asyncio
async def test_agent_state_dispatches_parameterized_actions():
    runtime = _FakeManagedRuntime()
    app = create_mcp_app(agent_id="loom", runtime=runtime)
    tool = app._tool_fns["agent_state"]

    assert await tool(action="list", include_offline=True) == [{"agent_id": "loom"}]
    assert await tool(action="get", agent_id="wren") == {"agent_id": "wren"}
    assert await tool(
        action="update",
        status="working",
        working_set=["repo"],
    ) == {"agent_id": "loom"}

    runtime.list_states.assert_awaited_once_with(include_offline=True)
    runtime.get_state.assert_awaited_once_with("wren")
    runtime.update_state.assert_awaited_once_with(
        status="working",
        working_set=["repo"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"action": "get"}, "requires agent_id"),
        ({"action": "list", "status": "nope"}, "only accepts include_offline"),
        ({"action": "update", "agent_id": "wren", "status": "x"}, "cannot target"),
        ({"action": "update"}, "requires status or working_set"),
    ],
)
async def test_agent_state_rejects_invalid_parameter_combinations(kwargs, message):
    app = create_mcp_app(agent_id="loom", runtime=_FakeManagedRuntime())

    with pytest.raises(ValueError, match=message):
        await app._tool_fns["agent_state"](**kwargs)
