# src/swarmbus/message.py
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

_AGENT_ID_SEGMENT = r"[a-z0-9][a-z0-9_-]*"
_AGENT_ID_RE = re.compile(
    rf"^{_AGENT_ID_SEGMENT}(?:\.{_AGENT_ID_SEGMENT})?$"
)
_MAX_AGENT_ID_LENGTH = 64
_MAX_BODY_BYTES = 64 * 1024  # 64KB

# Reserved — these collide with topic semantics and cannot be used as an
# agent's own registered ID. They are still valid as a `to` field on a
# message envelope (e.g. `to="broadcast"` routes to the broadcast topic).
RESERVED_AGENT_IDS = frozenset({"broadcast", "system"})


def _validate_agent_id(v: str) -> str:
    """Validate an agent ID with at most one topic-level separator."""
    if len(v) > _MAX_AGENT_ID_LENGTH or not _AGENT_ID_RE.fullmatch(v):
        raise ValueError(
            "Agent ID must match one or two dot-separated lowercase "
            "segments using letters, digits, hyphens, or underscores "
            f"(1-{_MAX_AGENT_ID_LENGTH} chars), got: {v!r}"
        )
    return v


def split_agent_id(v: str) -> tuple[str, ...]:
    """Return the one or two MQTT topic levels for an agent ID."""
    return tuple(_validate_agent_id(v).split("."))


def _validate_registered_agent_id(v: str) -> str:
    """Validate a registered identity and reject topic sentinels."""
    _validate_agent_id(v)
    if v in RESERVED_AGENT_IDS:
        raise ValueError(
            f"agent_id {v!r} is reserved (collides with a topic sentinel). "
            f"Reserved: {sorted(RESERVED_AGENT_IDS)}"
        )
    return v


class AgentMessage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    from_agent: str = Field(alias="from")
    to: str
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    subject: str
    body: str
    content_type: str = "text/plain"
    # `priority` is kept as `str` (not `Literal[...]`) on purpose: the wire
    # envelope must be forward/backward compatible across rolling upgrades.
    # Known values are "low", "normal", "high" (the wake wrappers gate on
    # "high"). Any other string passes through — callers may log a warning
    # if they don't recognise it, but MUST NOT drop the message. See
    # https://github.com/mpesavento/swarmbus — historical gotcha: an earlier
    # version used Literal["normal", "urgent"], which meant priority="high"
    # from a newer peer was silently discarded by older daemons.
    priority: str = "normal"
    reply_to: Optional[str] = None

    @field_validator("from_agent", mode="before")
    @classmethod
    def validate_from(cls, v: str) -> str:
        return _validate_agent_id(v)

    @field_validator("to", mode="before")
    @classmethod
    def validate_to(cls, v: str) -> str:
        return _validate_agent_id(v)

    @field_validator("body", mode="before")
    @classmethod
    def validate_body_size(cls, v: str) -> str:
        if len(v.encode("utf-8")) > _MAX_BODY_BYTES:
            raise ValueError(f"Body exceeds {_MAX_BODY_BYTES} bytes")
        return v

    @classmethod
    def create(
        cls,
        from_: str,
        to: str,
        subject: str,
        body: str,
        content_type: str = "text/plain",
        priority: str = "normal",
        reply_to: Optional[str] = None,
    ) -> "AgentMessage":
        return cls.model_validate({
            "from": from_,
            "to": to,
            "subject": subject,
            "body": body,
            "content_type": content_type,
            "priority": priority,
            "reply_to": reply_to,
        })

    def to_json(self) -> str:
        data = self.model_dump(by_alias=True)
        data["ts"] = self.ts.isoformat()
        return json.dumps(data)

    @classmethod
    def from_json(cls, raw: str | bytes) -> "AgentMessage":
        data = json.loads(raw)
        return cls.model_validate(data)
