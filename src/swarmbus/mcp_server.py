"""MCP sidecar for durable swarmbus messaging and agent state."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Literal

import aiomqtt

from .bus import AgentBus
from .runtime import ManagedMCPRuntime

logger = logging.getLogger(__name__)

try:
    from mcp.server.fastmcp import FastMCP
    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False
    FastMCP = None  # type: ignore[assignment,misc]


class _MCPApp:
    """Thin wrapper that tracks registered tool functions for testing."""

    def __init__(self) -> None:
        self._tool_fns: dict[str, Any] = {}

    def tool(self, fn=None, *, name: str | None = None):
        def decorator(f):
            key = name or f.__name__
            self._tool_fns[key] = f
            return f

        return decorator(fn) if fn else decorator


class _AgentBusRuntimeAdapter:
    """Compatibility path for callers that use create_mcp_app as a test seam."""

    def __init__(self, bus: AgentBus) -> None:
        self.bus = bus

    async def send_message(self, **kwargs) -> None:
        await self.bus.send(**kwargs)

    async def read_inbox(self) -> list[dict]:
        return await self.bus.read_inbox()

    async def watch_inbox(self, *, timeout: float) -> dict | None:
        return await self.bus.watch_inbox(timeout=timeout)

    async def list_agents(self) -> list[str]:
        return await self.bus.list_agents()

    async def list_states(self, *, include_offline: bool) -> list[dict]:
        raise RuntimeError("agent_state requires the managed MCP runtime")

    async def get_state(self, agent_id: str) -> dict:
        raise RuntimeError("agent_state requires the managed MCP runtime")

    async def update_state(
        self,
        *,
        status: str | None,
        working_set: list[str] | None,
    ) -> dict:
        raise RuntimeError("agent_state requires the managed MCP runtime")


def create_mcp_app(
    agent_id: str,
    broker: str = "localhost",
    port: int = 1883,
    *,
    persistent: bool = False,
    presence: bool = False,
    username: str | None = None,
    password: str | None = None,
    tls: bool = False,
    ca_cert: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
    runtime: Any | None = None,
) -> _MCPApp:
    """Create the tool surface, optionally over an injected managed runtime."""
    if runtime is None:
        bus = AgentBus(
            agent_id=agent_id,
            broker=broker,
            port=port,
            persistent=persistent,
            username=username,
            password=password,
            tls=tls,
            ca_cert=ca_cert,
            client_cert=client_cert,
            client_key=client_key,
        )
        runtime = _AgentBusRuntimeAdapter(bus)

    app = _MCPApp()

    @app.tool(name="send_message")
    async def send_message(
        to: str,
        subject: str,
        body: str,
        content_type: str = "text/plain",
        priority: str = "normal",
        reply_to: str | None = None,
    ) -> str:
        """Send a message to a peer or broadcast.

        priority defaults to "normal"; known values are "low", "normal", and
        "high", while unknown strings remain wire-compatible. reply_to is the
        agent ID that should receive a response when it differs from the sender.
        """
        await runtime.send_message(
            to=to,
            subject=subject,
            body=body,
            content_type=content_type,
            priority=priority,
            reply_to=reply_to,
        )
        return f"Sent to {to}"

    @app.tool(name="read_inbox")
    async def read_inbox() -> list[dict]:
        """Consume up to 10 pending messages from the durable local inbox."""
        try:
            return await runtime.read_inbox()
        except aiomqtt.MqttError as exc:
            logger.error(
                "read_inbox: broker error (%s:%d): %s",
                broker,
                port,
                exc,
            )
            return []

    @app.tool(name="watch_inbox")
    async def watch_inbox(timeout: float = 30.0) -> dict | None:
        """Wait for one durable inbox message, returning None on timeout."""
        try:
            return await runtime.watch_inbox(timeout=timeout)
        except aiomqtt.MqttError as exc:
            logger.error(
                "watch_inbox: broker error (%s:%d): %s",
                broker,
                port,
                exc,
            )
            return None

    # The `list_agents` MCP tool was REMOVED 2026-08-25. `agent_state` is the
    # single registry surface; it returns everything list_agents did plus
    # status, working set, heartbeat freshness and offline reason.
    #
    # Removed rather than deprecated because the failure was SILENT. The tool
    # took no parameters, but callers reasonably passed `include_offline=True`
    # (the name of the equivalent agent_state parameter) and got an online-only
    # result back with NO error -- a partial answer shaped like a total one.
    # That misled the same agent into the same wrong conclusion twice, six days
    # apart: "this peer was never registered", when it was a scheduled job that
    # was merely offline between runs. The second time it returned 2 of 8
    # agents and the caller went on to hand-guess peer IDs.
    #
    # A docstring deprecation would not have helped -- nobody reads a docstring
    # for a call that appears to have worked. `AgentBus.list_agents()` and the
    # `swarmbus list` CLI remain; the CLI grew online/offline querying in the
    # same change, so no capability was lost.

    @app.tool(name="agent_state")
    async def agent_state(
        action: Literal["list", "get", "update"],
        agent_id: str | None = None,
        status: str | None = None,
        working_set: list[str] | None = None,
        include_offline: bool = False,
    ) -> list[dict] | dict:
        """List, get, or update retained agent registry state.

        action="list" accepts only include_offline. action="get" requires only
        agent_id. action="update" changes this agent only, rejects agent_id, and
        requires status, working_set, or both.

        status is free-form Unicode text up to 280 characters. working_set is an
        awareness-only list of opaque strings and never reserves or locks
        anything. Omitted update fields remain unchanged; status="" and
        working_set=[] clear their fields.
        """
        if action == "list":
            if agent_id is not None or status is not None or working_set is not None:
                raise ValueError("list only accepts include_offline")
            return await runtime.list_states(include_offline=include_offline)

        if action == "get":
            if agent_id is None:
                raise ValueError("get requires agent_id")
            if status is not None or working_set is not None or include_offline:
                raise ValueError("get only accepts agent_id")
            return await runtime.get_state(agent_id)

        if action == "update":
            if agent_id is not None:
                raise ValueError("update cannot target another agent")
            if include_offline:
                raise ValueError("update does not accept include_offline")
            if status is None and working_set is None:
                raise ValueError("update requires status or working_set")
            return await runtime.update_state(
                status=status,
                working_set=working_set,
            )

        raise ValueError(f"unknown action {action!r}")

    return app


def run_mcp_server(
    agent_id: str,
    broker: str = "localhost",
    port: int = 1883,
    *,
    persistent: bool = False,
    presence: bool = False,
    state_dir: str = "~/.local/state/swarmbus",
    registry_heartbeat_seconds: float = 60,
    registry_stale_after_seconds: float = 180,
    username: str | None = None,
    password: str | None = None,
    tls: bool = False,
    ca_cert: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
) -> None:
    """Start the lifespan-managed MCP sidecar."""
    if not _MCP_AVAILABLE:
        raise RuntimeError(
            "mcp package not installed. Run: uv pip install 'swarmbus[mcp]'"
        )

    from mcp.server.fastmcp import FastMCP

    state_path = Path(state_dir).expanduser() / f"{agent_id}.sqlite3"
    runtime = ManagedMCPRuntime(
        agent_id=agent_id,
        broker=broker,
        port=port,
        persistent=persistent,
        presence=presence,
        state_path=state_path,
        heartbeat_seconds=registry_heartbeat_seconds,
        stale_after_seconds=registry_stale_after_seconds,
        username=username,
        password=password,
        tls=tls,
        ca_cert=ca_cert,
        client_cert=client_cert,
        client_key=client_key,
    )

    @asynccontextmanager
    async def lifespan(server: FastMCP) -> AsyncIterator[dict]:
        await runtime.start()
        try:
            yield {"runtime": runtime}
        finally:
            await runtime.stop()

    mcp = FastMCP("swarmbus", lifespan=lifespan)
    app = create_mcp_app(
        agent_id=agent_id,
        broker=broker,
        port=port,
        persistent=persistent,
        presence=presence,
        username=username,
        password=password,
        tls=tls,
        ca_cert=ca_cert,
        client_cert=client_cert,
        client_key=client_key,
        runtime=runtime,
    )
    for name, fn in app._tool_fns.items():
        mcp.tool(name=name)(fn)

    mcp.run(transport="stdio")
