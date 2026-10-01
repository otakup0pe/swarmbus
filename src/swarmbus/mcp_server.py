"""MCP sidecar for durable swarmbus messaging and agent state."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Literal

import aiomqtt

from .bus import AgentBus
from .registry import RegistryRecord
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

    async def list_states(
        self,
        *,
        include_offline: bool,
        lifecycle: Literal["persistent", "transient"] | None,
    ) -> list[dict]:
        raise RuntimeError("agent_state requires the managed MCP runtime")

    async def get_state(self, agent_id: str) -> dict:
        raise RuntimeError("agent_state requires the managed MCP runtime")

    async def update_state(
        self,
        *,
        status: str | None,
        working_set: list[str] | None,
        capabilities: list[str] | None,
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
    topic_root: str = "",
    runtime: Any | None = None,
) -> _MCPApp:
    """Create the tool surface, optionally over an injected managed runtime.

    ``topic_root`` is ignored when ``runtime`` is injected -- the caller
    built that runtime and already chose its topic layout. It applies only
    to the AgentBus constructed here.
    """
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
            topic_root=topic_root,
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

    @app.tool(name="list_agents")
    async def list_agents() -> list[str]:
        """Return IDs of agents currently online.

        This compatibility view remains stable in this release. Use
        agent_state(action="list") when status or heartbeat detail matters.
        """
        try:
            return await runtime.list_agents()
        except aiomqtt.MqttError as exc:
            logger.warning("list_agents: broker error: %s", exc)
            return []

    @app.tool(name="agent_state")
    async def agent_state(
        action: Literal["list", "get", "update"],
        agent_id: str | None = None,
        status: str | None = None,
        working_set: list[str] | None = None,
        capabilities: list[str] | None = None,
        include_offline: bool = False,
        lifecycle: Literal["persistent", "transient"] | None = None,
    ) -> list[dict] | dict:
        """List, get, or update retained agent registry state.

        action="list" accepts include_offline and an optional lifecycle filter.
        action="get" requires only agent_id. action="update" changes this agent
        only, rejects agent_id and lifecycle, and requires at least one mutable
        field.

        status is free-form Unicode text up to 280 characters. working_set is an
        awareness-only list of opaque strings and never reserves or locks
        anything. capabilities are advisory routing claims, not authorization.
        Omitted update fields remain unchanged; empty values clear their fields.
        """
        if action == "list":
            if (
                agent_id is not None
                or status is not None
                or working_set is not None
                or capabilities is not None
            ):
                raise ValueError(
                    "list only accepts include_offline and lifecycle"
                )
            return await runtime.list_states(
                include_offline=include_offline,
                lifecycle=lifecycle,
            )

        if action == "get":
            if agent_id is None:
                raise ValueError("get requires agent_id")
            if (
                status is not None
                or working_set is not None
                or capabilities is not None
                or include_offline
                or lifecycle is not None
            ):
                raise ValueError("get only accepts agent_id")
            return await runtime.get_state(agent_id)

        if action == "update":
            if agent_id is not None:
                raise ValueError("update cannot target another agent")
            if include_offline or lifecycle is not None:
                raise ValueError(
                    "update does not accept include_offline or lifecycle"
                )
            if (
                status is None
                and working_set is None
                and capabilities is None
            ):
                raise ValueError(
                    "update requires status, working_set, or capabilities"
                )
            return await runtime.update_state(
                status=status,
                working_set=working_set,
                capabilities=capabilities,
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
    lifecycle: Literal["persistent", "transient"] = "persistent",
    client_id: str | None = None,
    capabilities: tuple[str, ...] = (),
    state_dir: str = "~/.local/state/swarmbus",
    registry_heartbeat_seconds: float = 60,
    registry_stale_after_seconds: float = 180,
    username: str | None = None,
    password: str | None = None,
    tls: bool = False,
    ca_cert: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
    topic_root: str = "",
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
        lifecycle=lifecycle,
        client_id=client_id,
        state_path=state_path,
        heartbeat_seconds=registry_heartbeat_seconds,
        stale_after_seconds=registry_stale_after_seconds,
        username=username,
        password=password,
        tls=tls,
        ca_cert=ca_cert,
        client_cert=client_cert,
        client_key=client_key,
        topic_root=topic_root,
    )
    if capabilities:
        # Validate and normalize through the public registry model before the
        # runtime connects; no partially valid announcement reaches MQTT.
        runtime.declared_capabilities = RegistryRecord(
            agent_id=agent_id,
            capabilities=list(capabilities),
            lifecycle=lifecycle,
            started_at=runtime.started_at,
            last_seen=runtime.started_at,
        ).capabilities

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
