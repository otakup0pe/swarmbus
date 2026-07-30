from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiosqlite

from .handlers.base import BaseHandler
from .message import AgentMessage

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS messages (
    id           TEXT PRIMARY KEY,
    from_agent   TEXT NOT NULL,
    to_agent     TEXT NOT NULL,
    ts           TEXT NOT NULL,
    subject      TEXT NOT NULL,
    body         TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'text/plain',
    priority     TEXT NOT NULL DEFAULT 'normal',
    reply_to     TEXT,
    direction    TEXT NOT NULL DEFAULT 'received',
    error        TEXT,
    received_at  TEXT,
    source_topic TEXT,
    consumed_at  TEXT
)
"""

_MIGRATION_COLUMNS = {
    "received_at": "TEXT",
    "source_topic": "TEXT",
    "consumed_at": "TEXT",
}

_MESSAGE_COLUMNS = """
id, from_agent, to_agent, ts, subject, body, content_type, priority, reply_to
"""


class SQLiteMessageStore:
    """Durable inbound message store shared by MCP delivery and archives."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser()
        self._drain_lock = asyncio.Lock()
        self._opened = False

    async def open(self) -> None:
        if self._opened:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(_CREATE_TABLE)
            async with db.execute("PRAGMA table_info(messages)") as cursor:
                columns = {row[1] for row in await cursor.fetchall()}
            for name, sql_type in _MIGRATION_COLUMNS.items():
                if name not in columns:
                    await db.execute(
                        f"ALTER TABLE messages ADD COLUMN {name} {sql_type}"
                    )
            await db.execute("PRAGMA user_version = 1")
            await db.commit()
        self._opened = True

    async def close(self) -> None:
        self._opened = False

    async def _ensure_open(self) -> None:
        if not self._opened:
            await self.open()

    async def store(
        self,
        msg: AgentMessage,
        *,
        source_topic: str,
        received_at: datetime | None = None,
    ) -> bool:
        """Commit one inbound message, returning whether it was newly inserted."""
        return await self.archive(
            msg,
            direction="received",
            source_topic=source_topic,
            received_at=received_at,
        )

    async def archive(
        self,
        msg: AgentMessage,
        *,
        direction: str = "received",
        error: str | None = None,
        source_topic: str | None = None,
        received_at: datetime | None = None,
    ) -> bool:
        await self._ensure_open()
        received = received_at or datetime.now(timezone.utc)
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT INTO messages
                   (id, from_agent, to_agent, ts, subject, body,
                    content_type, priority, reply_to, direction, error,
                    received_at, source_topic)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO NOTHING""",
                (
                    msg.id,
                    msg.from_agent,
                    msg.to,
                    msg.ts.isoformat(),
                    msg.subject,
                    msg.body,
                    msg.content_type,
                    msg.priority,
                    msg.reply_to,
                    direction,
                    error,
                    received.isoformat(),
                    source_topic,
                ),
            )
            await db.commit()
            return cursor.rowcount == 1

    async def drain(self, max_messages: int = 10) -> list[dict]:
        """Atomically consume unread inbound rows oldest-first."""
        if max_messages < 1:
            raise ValueError("max_messages must be at least 1")
        await self._ensure_open()
        async with self._drain_lock:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                await db.execute("BEGIN IMMEDIATE")
                try:
                    async with db.execute(
                        f"""SELECT {_MESSAGE_COLUMNS}
                            FROM messages
                            WHERE direction = 'received'
                              AND consumed_at IS NULL
                            ORDER BY COALESCE(received_at, ts), rowid
                            LIMIT ?""",
                        (max_messages,),
                    ) as cursor:
                        rows = await cursor.fetchall()

                    if rows:
                        consumed_at = datetime.now(timezone.utc).isoformat()
                        await db.executemany(
                            """UPDATE messages
                               SET consumed_at = ?
                               WHERE id = ? AND consumed_at IS NULL""",
                            [(consumed_at, row["id"]) for row in rows],
                        )
                    await db.commit()
                except Exception:
                    await db.rollback()
                    raise

        return [
            json.loads(
                AgentMessage(
                    id=row["id"],
                    **{
                        "from": row["from_agent"],
                        "to": row["to_agent"],
                        "ts": row["ts"],
                        "subject": row["subject"],
                        "body": row["body"],
                        "content_type": row["content_type"],
                        "priority": row["priority"],
                        "reply_to": row["reply_to"],
                    },
                ).to_json()
            )
            for row in rows
        ]


class SQLiteArchive(BaseHandler):
    """Compatibility facade that archives handled messages in SQLite."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path).expanduser()
        self._store = SQLiteMessageStore(self.db_path)

    async def handle(self, msg: AgentMessage) -> None:
        await self.archive(msg, direction="received")

    async def archive(
        self,
        msg: AgentMessage,
        direction: str = "received",
        error: Optional[str] = None,
    ) -> None:
        await self._store.archive(msg, direction=direction, error=error)
