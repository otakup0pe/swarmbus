import pytest
from pathlib import Path
from swarmbus.archive import SQLiteArchive
from swarmbus.message import AgentMessage


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def msg():
    return AgentMessage.create(
        from_="wren", to="sparrow",
        subject="archive test", body="stored forever",
    )


@pytest.mark.asyncio
async def test_creates_table_and_stores_message(db_path, msg):
    archive = SQLiteArchive(db_path)
    await archive.handle(msg)

    import aiosqlite
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT * FROM messages WHERE id = ?", (msg.id,)) as cur:
            row = await cur.fetchone()
    assert row is not None
    assert row[1] == "wren"   # from_agent
    assert row[5] == "stored forever"  # body


@pytest.mark.asyncio
async def test_direction_defaults_to_received(db_path, msg):
    archive = SQLiteArchive(db_path)
    await archive.handle(msg)

    import aiosqlite
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT direction FROM messages WHERE id = ?", (msg.id,)) as cur:
            row = await cur.fetchone()
    assert row[0] == "received"


@pytest.mark.asyncio
async def test_stores_content_type(db_path):
    archive = SQLiteArchive(db_path)
    msg = AgentMessage.create(
        from_="wren", to="sparrow", subject="md", body="# Hello",
        content_type="text/markdown",
    )
    await archive.handle(msg)

    import aiosqlite
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT content_type FROM messages WHERE id = ?", (msg.id,)) as cur:
            row = await cur.fetchone()
    assert row[0] == "text/markdown"


@pytest.mark.asyncio
async def test_idempotent_on_duplicate_id(db_path, msg):
    archive = SQLiteArchive(db_path)
    await archive.handle(msg)
    await archive.handle(msg)  # same id — must not raise

    import aiosqlite
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT COUNT(*) FROM messages WHERE id = ?", (msg.id,)) as cur:
            row = await cur.fetchone()
    assert row[0] == 1


@pytest.mark.asyncio
async def test_creates_parent_dirs(tmp_path, msg):
    db_path = str(tmp_path / "nested" / "dir" / "archive.db")
    archive = SQLiteArchive(db_path)
    await archive.handle(msg)
    assert Path(db_path).exists()


@pytest.mark.asyncio
async def test_message_store_drains_unread_once(db_path, msg):
    from swarmbus.archive import SQLiteMessageStore

    store = SQLiteMessageStore(db_path)
    await store.open()
    try:
        inserted = await store.store(msg, source_topic="agents/sparrow/inbox")
        assert inserted is True

        first = await store.drain()
        second = await store.drain()

        assert [item["id"] for item in first] == [msg.id]
        assert second == []
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_duplicate_delivery_does_not_reset_consumed_state(db_path, msg):
    from swarmbus.archive import SQLiteMessageStore

    store = SQLiteMessageStore(db_path)
    await store.open()
    try:
        assert await store.store(msg, source_topic="agents/sparrow/inbox") is True
        assert len(await store.drain()) == 1

        assert await store.store(msg, source_topic="agents/sparrow/inbox") is False
        assert await store.drain() == []
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_concurrent_drains_do_not_return_same_message(db_path, msg):
    import asyncio
    from swarmbus.archive import SQLiteMessageStore

    store = SQLiteMessageStore(db_path)
    await store.open()
    try:
        await store.store(msg, source_topic="agents/sparrow/inbox")
        left, right = await asyncio.gather(
            store.drain(max_messages=1),
            store.drain(max_messages=1),
        )
        assert len(left) + len(right) == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_message_store_migrates_existing_archive_schema(db_path, msg):
    import aiosqlite
    from swarmbus.archive import SQLiteMessageStore

    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                from_agent TEXT NOT NULL,
                to_agent TEXT NOT NULL,
                ts TEXT NOT NULL,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                content_type TEXT NOT NULL DEFAULT 'text/plain',
                priority TEXT NOT NULL DEFAULT 'normal',
                reply_to TEXT,
                direction TEXT NOT NULL DEFAULT 'received',
                error TEXT
            )"""
        )
        await db.execute(
            """INSERT INTO messages
               (id, from_agent, to_agent, ts, subject, body,
                content_type, priority, reply_to, direction)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'received')""",
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
            ),
        )
        await db.commit()

    store = SQLiteMessageStore(db_path)
    await store.open()
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute("PRAGMA table_info(messages)") as cursor:
                columns = {row[1] for row in await cursor.fetchall()}
            async with db.execute("PRAGMA user_version") as cursor:
                user_version = (await cursor.fetchone())[0]

        assert {"received_at", "source_topic", "consumed_at"} <= columns
        assert user_version == 1
        assert [item["id"] for item in await store.drain()] == [msg.id]
    finally:
        await store.close()
