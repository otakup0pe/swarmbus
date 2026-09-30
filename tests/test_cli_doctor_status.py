"""doctor's status vocabulary: ok / warn / fail / skip / unknown.

The behaviour under test is an EXIT-CODE contract, not cosmetics.

Before 2026-09-26 ``doctor`` overloaded ``"skip"`` to mean both "this
check does not apply here" and "this check was supposed to run and it
broke". Neither incremented ``fails`` or ``warns``, so a check that blew
up still fell through to ``[doctor] all green.`` and ``sys.exit(0)``.
The reason it broke was printed on its own line, and then the summary
line and the process exit code both contradicted it. An operator
scripting on ``$?`` -- which is the entire reason doctor has distinct
exit codes -- got a green light on a daemon nobody had verified.

The split gives "unknown" its own counter and its own summary sentence.
These tests pin all three halves of that:

* a check that RAISES renders as unknown, the summary refuses to say
  green, and the exit code stays 0;
* a genuinely not-applicable check still renders as skip and still
  reaches "all green" with exit 0 -- this is the half that proves the
  change distinguished the two cases rather than renaming both;
* a real failure still exits 1, and an unknown alongside it does not
  soften that.

What is faked, and what is not
------------------------------
Three external resources, and nothing else: the MQTT broker
(``aiomqtt.Client``), systemd (``subprocess.run``), and ``/proc``
(``Path.read_text``, denied only for ``/proc`` paths so every other read
goes to the real filesystem). ``AgentBus``, ``AgentBus.probe``,
``TopicMap`` and the whole doctor body are the real implementations, so
every assertion below reads what doctor actually rendered and the code
it actually exited with.

Assertions are on the rendered output and ``result.exit_code``, never on
doctor's internal counters -- the counters are not the contract, the
printed verdict and ``$?`` are.
"""
from __future__ import annotations

import json
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from swarmbus.cli import main
from swarmbus.topics import TopicMap

#: The agent doctor is asked about. Test input, not a value from src.
AGENT_ID = "doc-agent"

#: Status glyphs doctor renders. Spelled with chr() so this file stays
#: ASCII while the assertions still compare against the real non-ASCII
#: characters the command prints.
SKIP_ICON = chr(0x00B7)  # MIDDLE DOT
UNKNOWN_ICON = "?"

#: A PID doctor will try to inspect under /proc once systemd reports the
#: unit as active.
FAKE_MAIN_PID = "4242"


@pytest.fixture(autouse=True)
def _no_ambient_swarmbus_env(monkeypatch):
    """Strip every ``SWARMBUS_*`` var for the duration of each test.

    A developer shell exporting ``SWARMBUS_OUTBOX_DOC_AGENT`` would win
    over the per-test outbox (doctor checks the agent-scoped name first)
    and turn check 6 red for a reason that has nothing to do with the
    status vocabulary.
    """
    for key in list(os.environ):
        if key.startswith("SWARMBUS_"):
            monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# fakes -- broker, systemd, /proc
# ---------------------------------------------------------------------------


class _PresenceMessage:
    """What ``list_agents`` iterates: a topic and a JSON payload."""

    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class _FakeClient:
    """``aiomqtt.Client`` stand-in.

    Connecting always succeeds, which is what makes doctor's check 2 go
    green. ``messages`` replays a fixed list and then ends, so
    ``list_agents`` falls out of its collect loop immediately instead of
    burning its 0.5s window.
    """

    def __init__(self, opened: list, messages: list, *args, **kwargs) -> None:
        self._messages = messages
        self.init_args = args
        self.init_kwargs = kwargs
        opened.append(self)

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    async def publish(self, topic, payload=None, **_kwargs) -> None:
        return None

    async def subscribe(self, topic, **_kwargs) -> None:
        return None

    @property
    def messages(self):
        async def _replay():
            for message in self._messages:
                yield message

        return _replay()


@contextmanager
def _fake_broker(online=()):
    """Reachable broker that reports ``online`` as the live agents.

    Both ``swarmbus.bus`` and ``swarmbus.cli`` do a plain ``import
    aiomqtt`` and reach through the module object, so patching the
    attribute once covers doctor's inline reachability probe as well as
    the bus's ``list_agents``.

    Presence topics come from ``TopicMap`` rather than a literal, so the
    fixture follows the real wire layout.
    """
    topics = TopicMap()
    messages = [
        _PresenceMessage(
            topics.presence(name),
            json.dumps({"agent": name, "status": "online"}).encode(),
        )
        for name in online
    ]
    opened: list[_FakeClient] = []

    def _factory(*args, **kwargs):
        return _FakeClient(opened, messages, *args, **kwargs)

    with patch("swarmbus.bus.aiomqtt.Client", side_effect=_factory):
        yield opened


@contextmanager
def _systemctl_absent():
    """No systemd at all -- doctor's genuine not-applicable path.

    ``systemctl`` missing makes check 3 skip and leaves checks 4 and 5
    with nothing to inspect, so they skip too. Three not-applicable
    checks, zero broken ones.
    """
    with patch(
        "subprocess.run",
        side_effect=FileNotFoundError(2, "No such file or directory", "systemctl"),
    ):
        yield


@contextmanager
def _systemctl_reports_active_unit():
    """A running unit, so checks 4 and 5 become APPLICABLE.

    This is the setup that separates unknown from skip: with a live
    MainPID, the staleness check has something it is supposed to verify,
    and failing to verify it is no longer "not applicable".
    """
    stdout = "\n".join([
        "ActiveState=active",
        "SubState=running",
        f"MainPID={FAKE_MAIN_PID}",
        "ExecMainStartTimestamp=Sat 2026-09-26 08:00:00 PDT",
        "ExecStart={ path=/usr/bin/swarmbus ; argv[]=/usr/bin/swarmbus start "
        f"--agent-id {AGENT_ID} --invoke /opt/wake ; ignore_errors=no }}",
    ])
    completed = subprocess.CompletedProcess(
        args=[], returncode=0, stdout=stdout, stderr=""
    )
    with patch("subprocess.run", return_value=completed):
        yield


@contextmanager
def _proc_is_unreadable():
    """Deny reads under /proc, the way ``hidepid=2`` does.

    This is the real-world shape of a check that BREAKS rather than one
    that does not apply: the daemon exists, its staleness is a real
    question, and doctor cannot answer it. Reads outside /proc are
    delegated to the genuine ``Path.read_text``.
    """
    real_read_text = Path.read_text

    def _denied(self, *args, **kwargs):
        if str(self).startswith("/proc/"):
            raise PermissionError(13, "Permission denied", str(self))
        return real_read_text(self, *args, **kwargs)

    with patch.object(Path, "read_text", _denied):
        yield


def _invoke_doctor(outbox: Path):
    """Run ``doctor`` with a writable outbox so check 6 is green.

    An unset outbox is a WARNING, which would drag every scenario here
    off the summary branch it is meant to land on.
    """
    runner = CliRunner()
    return runner.invoke(
        main,
        ["doctor", "--agent-id", AGENT_ID],
        env={"SWARMBUS_OUTBOX": str(outbox)},
    )


# ---------------------------------------------------------------------------
# "unknown": the check was applicable and could not complete
# ---------------------------------------------------------------------------


def test_a_check_that_raises_is_unknown_and_the_summary_refuses_to_say_green(tmp_path):
    """The regression this split exists for.

    Everything else is green; only the staleness check breaks, on the
    exact failure (/proc unreadable) the old code filed under "skip".
    The summary must say the health is UNKNOWN instead of claiming all
    green, and it must not fold the unknown into the warning count.
    """
    outbox = tmp_path / "outbox.md"
    with _fake_broker(online=(AGENT_ID,)), \
            _systemctl_reports_active_unit(), \
            _proc_is_unreadable():
        result = _invoke_doctor(outbox)

    # The broken check renders as unknown, on its own glyph.
    assert f"[{UNKNOWN_ICON}] 4. daemon library fresh" in result.output, result.output
    assert "could not verify" in result.output

    # And NOT as skip -- the whole point is that these are different.
    assert SKIP_ICON not in result.output, result.output

    # The verdict an operator reads.
    assert (
        "[doctor] 1 check(s) could not be verified. Health is UNKNOWN, not green."
        in result.output
    ), result.output
    assert "[doctor] all green." not in result.output
    assert "all critical checks passed" not in result.output
    # No "; N warning(s) above" clause: an unknown is not a warning.
    assert "warning(s) above" not in result.output


def test_an_unknown_check_exits_3_so_scripts_can_tell_it_from_green(tmp_path):
    """Deliberate, and separated out so it is hard to change by accident.

    ⚰️ This test originally asserted exit 0, on the reasoning that
    unverified is not proven-broken and that failing would make doctor
    unusable wherever /proc is restricted. ⛔ That half-solved it: the
    ORIGINAL bug was that the machine-readable signal lied, and leaving
    $? at 0 left it lying -- an operator scripting on it still could not
    separate "all green" from "could not check" without grepping stdout,
    which is the thing exit codes exist to avoid.

    3 is distinct from 0 (green), 1 (something is red) and 2 (doctor
    itself could not run), so each caller picks its own policy: a CI
    gate fails on 3, a hardened host accepts `[ $? -le 3 ]`. No
    previously-green run changes code.

    🔑 Today the ONLY check that can yield unknown is daemon library
    freshness -- added because of the 2026-04-14 stale-code incident.
    So a 3 means precisely "the check we added because we got burned
    did not run". ⛔ Do not fold it back into 0.
    """
    outbox = tmp_path / "outbox.md"
    with _fake_broker(online=(AGENT_ID,)), \
            _systemctl_reports_active_unit(), \
            _proc_is_unreadable():
        result = _invoke_doctor(outbox)

    assert result.exit_code == 3, result.output
    assert "Health is UNKNOWN" in result.output


# ---------------------------------------------------------------------------
# "skip": not applicable -- still green
# ---------------------------------------------------------------------------


def test_a_not_applicable_check_still_skips_and_still_reaches_all_green(tmp_path):
    """The other half of the split.

    Without ``systemctl`` there is no unit, so three checks have nothing
    to look at. That is not a degraded install and must not be reported
    as one: still ``[doctor] all green.``, still exit 0, and no mention
    of anything being unverified.
    """
    outbox = tmp_path / "outbox.md"
    with _fake_broker(online=(AGENT_ID,)), _systemctl_absent():
        result = _invoke_doctor(outbox)

    assert result.exit_code == 0, result.output

    # Three genuinely-not-applicable checks, each on the skip glyph.
    assert "(systemctl not found)" in result.output, result.output
    assert "(no daemon to check)" in result.output
    assert "(no unit to inspect)" in result.output
    assert result.output.count(f"[{SKIP_ICON}]") == 3, result.output

    # None of them is unknown, and none of them costs the green light.
    assert f"[{UNKNOWN_ICON}]" not in result.output, result.output
    assert "could not be verified" not in result.output
    assert "UNKNOWN" not in result.output
    assert "[doctor] all green." in result.output


# ---------------------------------------------------------------------------
# "fail": unaffected by the split
# ---------------------------------------------------------------------------


def test_a_real_failure_still_exits_1(tmp_path):
    """The broker answers but this agent is not in the presence list."""
    outbox = tmp_path / "outbox.md"
    with _fake_broker(online=()), _systemctl_absent():
        result = _invoke_doctor(outbox)

    assert result.exit_code == 1, result.output
    assert "peer discovery.......... I'm NOT in the online list" in result.output
    assert "[doctor] 1 failure(s), 0 warning(s)" in result.output
    assert "[doctor] all green." not in result.output
    assert "Health is UNKNOWN" not in result.output


def test_an_unknown_alongside_a_failure_does_not_soften_the_exit_code(tmp_path):
    """A red check wins, and the summary still declares the unverified one.

    The failure branch is a separate sentence from the unknown branch,
    so it gets its own coverage: dropping the ``unverified`` clause there
    would hide a broken check behind an unrelated failure.
    """
    outbox = tmp_path / "outbox.md"
    with _fake_broker(online=()), \
            _systemctl_reports_active_unit(), \
            _proc_is_unreadable():
        result = _invoke_doctor(outbox)

    assert result.exit_code == 1, result.output
    assert "[doctor] 1 failure(s), 0 warning(s), 1 unverified" in result.output
    assert f"[{UNKNOWN_ICON}] 4. daemon library fresh" in result.output
