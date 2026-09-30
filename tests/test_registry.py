from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from swarmbus.registry import PresenceRecord, RegistryCache, RegistryRecord


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


def test_capabilities_are_normalized_but_remain_open_vocabulary():
    record = RegistryRecord.model_validate(
        {
            **_record("loom").model_dump(),
            "capabilities": [
                " development.files.write ",
                "yopo.facet.healthcheck",
                "development.files.write",
            ],
        }
    )

    assert record.capabilities == [
        "development.files.write",
        "yopo.facet.healthcheck",
    ]

    with pytest.raises(ValidationError, match="128"):
        RegistryRecord.model_validate(
            {
                **_record("loom").model_dump(),
                "capabilities": ["x" * 129],
            }
        )


def test_lifecycle_defaults_persistent_for_schema_v1_compatibility():
    payload = _record("loom").model_dump()
    payload.pop("lifecycle")

    record = RegistryRecord.model_validate(payload)

    assert record.lifecycle == "persistent"


def test_lifecycle_filters_keep_offline_persistent_directory_separate():
    cache = RegistryCache(stale_after_seconds=180)
    persistent = _record("healthcheck", age_seconds=181)
    transient = RegistryRecord.model_validate(
        {**_record("codex-session").model_dump(), "lifecycle": "transient"}
    )
    for record in (persistent, transient):
        cache.update_registry(
            f"swarmbus/registry/{record.agent_id}", record.to_json()
        )
        cache.update_presence(
            f"agents/{record.agent_id}/presence",
            PresenceRecord(
                agent_id=record.agent_id,
                state="online",
            ).to_json(),
        )

    assert cache.list_states(
        lifecycle="persistent", include_offline=True
    )[0]["agent_id"] == "healthcheck"
    assert [
        state["agent_id"]
        for state in cache.list_states(lifecycle="transient")
    ] == ["codex-session"]


def test_retained_tombstones_remove_transient_state():
    cache = RegistryCache(stale_after_seconds=180)
    record = RegistryRecord.model_validate(
        {**_record("codex-session").model_dump(), "lifecycle": "transient"}
    )
    registry_topic = "swarmbus/registry/codex-session"
    presence_topic = "agents/codex-session/presence"
    cache.update_registry(registry_topic, record.to_json())
    cache.update_presence(
        presence_topic,
        PresenceRecord(
            agent_id="codex-session", state="online"
        ).to_json(),
    )

    cache.remove_registry(registry_topic)
    cache.remove_presence(presence_topic)

    assert cache.list_states(include_offline=True) == []
