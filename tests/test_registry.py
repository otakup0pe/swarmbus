from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from swarmbus.registry import RegistryCache, RegistryRecord


def _record(agent_id: str, *, age_seconds: float = 0) -> RegistryRecord:
    now = datetime.now(timezone.utc)
    return RegistryRecord(
        agent_id=agent_id,
        status="building the weird little bus 🚌",
        working_set=[" repo ", "broker ACLs", "repo"],
        capabilities=["messaging", "agent-state"],
        durability="durable",
        started_at=now - timedelta(minutes=5),
        last_seen=now - timedelta(seconds=age_seconds),
    )


def test_status_is_freeform_and_working_set_is_opaque_strings():
    record = _record("loom")

    assert record.status == "building the weird little bus 🚌"
    assert record.working_set == ["repo", "broker ACLs"]


def test_status_limit_is_enforced():
    with pytest.raises(ValidationError, match="280"):
        _record("loom").model_copy(update={"status": "x" * 281})
        RegistryRecord.model_validate(
            {**_record("loom").model_dump(), "status": "x" * 281}
        )


def test_working_set_rejects_empty_entries():
    with pytest.raises(ValidationError, match="empty"):
        RegistryRecord.model_validate(
            {**_record("loom").model_dump(), "working_set": ["repo", " "]}
        )


def test_registry_topic_must_match_payload_agent():
    payload = _record("loom").to_json()

    with pytest.raises(ValueError, match="does not match"):
        RegistryRecord.from_mqtt("swarmbus/registry/wren", payload)


def test_cache_combines_presence_and_heartbeat_freshness():
    cache = RegistryCache(stale_after_seconds=180)
    fresh = _record("loom", age_seconds=30)
    stale = _record("wren", age_seconds=181)

    cache.update_registry("swarmbus/registry/loom", fresh.to_json())
    cache.update_registry("swarmbus/registry/wren", stale.to_json())
    cache.update_presence(
        "agents/loom/presence",
        '{"schema_version": 1, "agent_id": "loom", "state": "online"}',
    )
    cache.update_presence(
        "agents/wren/presence",
        '{"schema_version": 1, "agent_id": "wren", "state": "online"}',
    )

    assert cache.online_agent_ids() == ["loom"]
    assert [state["agent_id"] for state in cache.list_states()] == ["loom"]
    assert [state["agent_id"] for state in cache.list_states(include_offline=True)] == [
        "loom",
        "wren",
    ]


def test_legacy_presence_only_agent_remains_discoverable():
    cache = RegistryCache(stale_after_seconds=180)
    cache.update_presence(
        "agents/legacy/presence",
        '{"agent": "legacy", "status": "online"}',
    )

    assert cache.online_agent_ids() == ["legacy"]
    state = cache.get_state("legacy")
    assert state["status"] == ""
    assert state["working_set"] == []
    assert state["online"] is True


def test_offline_reason_is_exposed():
    cache = RegistryCache(stale_after_seconds=180)
    record = _record("loom")
    cache.update_registry("swarmbus/registry/loom", record.to_json())
    cache.update_presence(
        "agents/loom/presence",
        '{"schema_version": 1, "agent_id": "loom", "state": "offline", '
        '"reason": "clean-shutdown"}',
    )

    assert cache.get_state("loom")["offline_reason"] == "clean-shutdown"
