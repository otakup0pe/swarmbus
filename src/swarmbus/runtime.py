from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import aiomqtt

from .archive import SQLiteMessageStore
from .bus import _build_tls_context
from .message import AgentMessage, _validate_registered_agent_id
from .registry import PresenceRecord, RegistryCache, RegistryRecord

logger = logging.getLogger(__name__)


class _ManualAckAdapter:
    """Quarantine aiomqtt's private Paho handle until it grows a public API."""

    def __init__(self, client: aiomqtt.Client) -> None:
        try:
            self._paho = client._client
        except AttributeError as exc:
            raise RuntimeError(
                "aiomqtt does not expose the Paho client required for manual ACK"
            ) from exc
        if not hasattr(self._paho, "manual_ack_set") or not hasattr(
            self._paho, "ack"
        ):
            raise RuntimeError("installed Paho client does not support manual ACK")

    def enable(self) -> None:
        self._paho.manual_ack_set(True)

    def ack(self, message: Any) -> None:
        qos = int(message.qos)
        if qos == 0:
            return
        result = self._paho.ack(message.mid, qos)
        if result not in (0, None):
            raise RuntimeError(f"Paho ACK failed with result {result}")


class ManagedMCPRuntime:
    """One MQTT connection and durable inbox for an MCP server process."""

    def __init__(
        self,
        *,
        agent_id: str,
        broker: str = "localhost",
        port: int = 1883,
        persistent: bool = False,
        presence: bool = True,
        state_path: str | Path,
        heartbeat_seconds: float = 60,
        stale_after_seconds: float = 180,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        store: SQLiteMessageStore | Any | None = None,
    ) -> None:
        _validate_registered_agent_id(agent_id)
        if heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        if stale_after_seconds < heartbeat_seconds * 2:
            raise ValueError(
                "stale_after_seconds must be at least twice heartbeat_seconds"
            )
        self.agent_id = agent_id
        self.broker = broker
        self.port = port
        self.persistent = persistent
        self.presence = presence
        self.heartbeat_seconds = heartbeat_seconds
        self.username = username
        self.password = password
        self._tls_context = _build_tls_context(
            tls=tls,
            ca_cert=ca_cert,
            client_cert=client_cert,
            client_key=client_key,
        )
        self.store = store or SQLiteMessageStore(state_path)
        self.registry = RegistryCache(stale_after_seconds=stale_after_seconds)
        self.started_at = datetime.now(timezone.utc)
        self.status = ""
        self.working_set: list[str] = []
        self.capabilities = ["messaging", "durable-inbox", "agent-state"]
        self._client: aiomqtt.Client | None = None
        self._connection_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._ready = asyncio.Event()
        self._stopping = asyncio.Event()
        self._inbox_condition = asyncio.Condition()

    def _client_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "will": aiomqtt.Will(
                topic=f"agents/{self.agent_id}/presence",
                payload=PresenceRecord(
                    agent_id=self.agent_id,
                    state="offline",
                    reason="connection-lost",
                ).to_json(),
                qos=1,
                retain=True,
            )
        }
        if self.username is not None:
            kwargs["username"] = self.username
        if self.password is not None:
            kwargs["password"] = self.password
        if self._tls_context is not None:
            kwargs["tls_context"] = self._tls_context
        if self.persistent:
            kwargs["identifier"] = f"swarmbus-{self.agent_id}"
            kwargs["clean_session"] = False
        return kwargs

    async def start(self) -> None:
        await self.store.open()
        self._connection_task = asyncio.create_task(self._connection_loop())
        ready_wait = asyncio.create_task(self._ready.wait())
        done, pending = await asyncio.wait(
            {ready_wait, self._connection_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            if task is ready_wait:
                task.cancel()
        if self._connection_task in done:
            self._connection_task.result()

    async def stop(self) -> None:
        if self._stopping.is_set():
            return
        self._stopping.set()
        if self._client is not None and self.presence:
            await self._publish_presence("offline", reason="clean-shutdown")
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        if self._connection_task is not None:
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
        self._client = None
        await self.store.close()

    async def _connection_loop(self) -> None:
        backoff = 1.0
        while not self._stopping.is_set():
            client = aiomqtt.Client(
                self.broker,
                port=self.port,
                **self._client_kwargs(),
            )
            ack_adapter = _ManualAckAdapter(client)
            ack_adapter.enable()
            try:
                async with client:
                    self._client = client
                    await client.subscribe(
                        f"agents/{self.agent_id}/inbox", qos=1
                    )
                    await client.subscribe("agents/broadcast", qos=1)
                    await client.subscribe("agents/+/presence", qos=1)
                    await client.subscribe("swarmbus/registry/+", qos=1)
                    if self.presence:
                        await self._publish_presence("online")
                        await self._publish_registry()
                    self._ready.set()
                    backoff = 1.0
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop()
                    )
                    async for message in client.messages:
                        try:
                            await self._handle_message(message, ack_adapter)
                        except OSError as exc:
                            logger.error(
                                "inbox commit failed; reconnecting for redelivery: %s",
                                exc,
                            )
                            break
            except aiomqtt.MqttError as exc:
                if self._stopping.is_set():
                    break
                logger.warning(
                    "MQTT broker disconnected (%s); reconnecting in %.1fs",
                    exc,
                    backoff,
                )
            finally:
                self._client = None
                if self._heartbeat_task is not None:
                    self._heartbeat_task.cancel()
                    self._heartbeat_task = None

            if not self._stopping.is_set():
                await asyncio.sleep(backoff + random.uniform(0, backoff * 0.1))
                backoff = min(backoff * 2, 60)

    async def _heartbeat_loop(self) -> None:
        while not self._stopping.is_set():
            await asyncio.sleep(self.heartbeat_seconds)
            if self._client is not None and self.presence:
                try:
                    await self._publish_registry()
                except Exception as exc:
                    logger.warning("heartbeat registry publish failed: %s", exc)

    async def _handle_message(
        self,
        mqtt_message: Any,
        ack_adapter: _ManualAckAdapter | Any,
    ) -> None:
        topic = str(mqtt_message.topic)
        if topic in {
            f"agents/{self.agent_id}/inbox",
            "agents/broadcast",
        }:
            try:
                message = AgentMessage.from_json(mqtt_message.payload)
            except (TypeError, ValueError, UnicodeDecodeError) as exc:
                logger.warning("discarding invalid message envelope: %s", exc)
                ack_adapter.ack(mqtt_message)
                return
            await self.store.store(message, source_topic=topic)
            async with self._inbox_condition:
                self._inbox_condition.notify_all()
            ack_adapter.ack(mqtt_message)
            return

        if topic.startswith("swarmbus/registry/"):
            try:
                self.registry.update_registry(topic, mqtt_message.payload)
            except (TypeError, ValueError, UnicodeDecodeError) as exc:
                logger.warning("discarding invalid registry record: %s", exc)
            ack_adapter.ack(mqtt_message)
            return

        if topic.startswith("agents/") and topic.endswith("/presence"):
            try:
                self.registry.update_presence(topic, mqtt_message.payload)
            except (TypeError, ValueError, UnicodeDecodeError) as exc:
                logger.warning("discarding invalid presence record: %s", exc)
            ack_adapter.ack(mqtt_message)
            return

        ack_adapter.ack(mqtt_message)

    async def send_message(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        content_type: str = "text/plain",
        priority: str = "normal",
        reply_to: str | None = None,
    ) -> None:
        if self._client is None:
            raise RuntimeError("swarmbus MQTT runtime is not connected")
        message = AgentMessage.create(
            from_=self.agent_id,
            to=to,
            subject=subject,
            body=body,
            content_type=content_type,
            priority=priority,
            reply_to=reply_to,
        )
        topic = "agents/broadcast" if to == "broadcast" else f"agents/{to}/inbox"
        await self._client.publish(
            topic,
            message.to_json(),
            qos=1,
            retain=False,
        )

    async def read_inbox(self, max_messages: int = 10) -> list[dict]:
        return await self.store.drain(max_messages=max_messages)

    async def watch_inbox(self, timeout: float = 30.0) -> dict | None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            messages = await self.store.drain(max_messages=1)
            if messages:
                return messages[0]
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            async with self._inbox_condition:
                messages = await self.store.drain(max_messages=1)
                if messages:
                    return messages[0]
                try:
                    await asyncio.wait_for(
                        self._inbox_condition.wait(),
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    return None

    async def list_agents(self) -> list[str]:
        return self.registry.online_agent_ids()

    async def list_states(self, *, include_offline: bool = False) -> list[dict]:
        return self.registry.list_states(include_offline=include_offline)

    async def get_state(self, agent_id: str) -> dict:
        try:
            return self.registry.get_state(agent_id)
        except KeyError as exc:
            raise ValueError(f"no registry record for agent {agent_id!r}") from exc

    async def update_state(
        self,
        *,
        status: str | None,
        working_set: list[str] | None,
    ) -> dict:
        if status is None and working_set is None:
            raise ValueError("update requires status or working_set")
        candidate = RegistryRecord(
            agent_id=self.agent_id,
            status=self.status if status is None else status,
            working_set=self.working_set if working_set is None else working_set,
            capabilities=self.capabilities,
            durability="durable" if self.persistent else "ephemeral",
            started_at=self.started_at,
            last_seen=datetime.now(timezone.utc),
        )
        self.status = candidate.status
        self.working_set = candidate.working_set
        await self._publish_registry()
        return self.registry.get_state(self.agent_id)

    async def _publish_presence(
        self,
        state: Literal["online", "offline"] | str,
        *,
        reason: str | None = None,
    ) -> None:
        if self._client is None:
            raise RuntimeError("swarmbus MQTT runtime is not connected")
        record = PresenceRecord(
            agent_id=self.agent_id,
            state=state,
            connected_at=datetime.now(timezone.utc) if state == "online" else None,
            reason=reason,
        )
        topic = f"agents/{self.agent_id}/presence"
        await self._client.publish(topic, record.to_json(), qos=1, retain=True)
        self.registry.update_presence(topic, record.to_json())

    async def _publish_registry(self) -> None:
        if self._client is None:
            raise RuntimeError("swarmbus MQTT runtime is not connected")
        record = RegistryRecord(
            agent_id=self.agent_id,
            status=self.status,
            working_set=self.working_set,
            capabilities=self.capabilities,
            durability="durable" if self.persistent else "ephemeral",
            started_at=self.started_at,
            last_seen=datetime.now(timezone.utc),
        )
        topic = f"swarmbus/registry/{self.agent_id}"
        await self._client.publish(topic, record.to_json(), qos=1, retain=True)
        self.registry.update_registry(topic, record.to_json())
