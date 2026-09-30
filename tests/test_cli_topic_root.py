"""CLI surface tests for ``--topic-root`` on every command that talks
to the broker.

The failure this file exists to catch is silent. A deployment exports a
root, the sidecars move to ``loom/agents/...``, and one CLI command is
left unrooted on ``agents/...``. MQTT raises nothing for publishing or
subscribing where nobody is on the other end, so ``swarmbus list``
returns empty and ``swarmbus send`` drops messages -- precisely while an
operator is using them to diagnose something. A grep over the source
proves the option is *present*; only these tests prove it *arrives*.

What is faked here, and what is not
-----------------------------------
Only the broker: ``aiomqtt.Client`` is swapped for ``_RecordingClient``,
which writes down the literal topic strings it is handed. ``AgentBus``,
``AgentBus.probe``, ``TopicMap`` and the Click plumbing are all the real
implementations, so every assertion below reads the topic map the real
class resolved, not a mock's call kwargs. The one exception is
``_probe_spy``, which wraps ``AgentBus.probe`` to capture the real
instances it returns -- doctor's broker-reachability probe uses its bus
only for ``_aiomqtt_kwargs()``, so its root never reaches the wire and
the constructed object is the only place it can be observed at all.

Expectations come from the producer: ``TopicMap(root=...)`` builds the
strings the assertions compare against, so a change to the layout moves
the tests with it rather than leaving them asserting stale literals.
``tests/test_topics.py`` is what pins those literals.
"""
from __future__ import annotations

import ast
import inspect
import os
import subprocess
from contextlib import contextmanager
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

import swarmbus.cli
from swarmbus.bus import AgentBus
from swarmbus.cli import _topic_root_option, main
from swarmbus.topics import TopicMap

#: The namespace a rooted deployment would export. Test input, not a
#: value copied out of the implementation.
ROOT = "loom"

#: Commands that build a bus today. Used only as a floor, so a broken
#: detector cannot make the enumeration tests pass by finding nothing.
KNOWN_BUS_COMMANDS = {"send", "start", "read", "watch", "list", "doctor"}


@pytest.fixture(autouse=True)
def _no_ambient_swarmbus_env(monkeypatch):
    """Strip every ``SWARMBUS_*`` var for the duration of each test.

    The default-path tests assert the UNROOTED topics, so a developer
    shell exporting ``SWARMBUS_TOPIC_ROOT`` would turn them red for the
    wrong reason. A stray ``SWARMBUS_OUTBOX`` would also make ``send``
    append to a real file on disk.
    """
    for key in list(os.environ):
        if key.startswith("SWARMBUS_"):
            monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# broker fake
# ---------------------------------------------------------------------------


class _BrokerLog:
    """Everything the commands handed to the broker, in order."""

    def __init__(self) -> None:
        self.clients: list[_RecordingClient] = []
        self.published: list[str] = []
        self.subscribed: list[str] = []
        self.wills: list[str] = []


class _RecordingClient:
    """``aiomqtt.Client`` stand-in that records the real wire strings.

    Yields no messages, so ``read``/``watch``/``list`` fall straight out
    of their drain loops without waiting on a timeout and ``start``'s
    listen loop returns cleanly after subscribing.
    """

    def __init__(self, log: _BrokerLog, *args, **kwargs) -> None:
        self._log = log
        self.init_args = args
        self.init_kwargs = kwargs
        log.clients.append(self)
        will = kwargs.get("will")
        if will is not None:
            log.wills.append(will.topic)

    async def __aenter__(self) -> "_RecordingClient":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    async def publish(self, topic, payload=None, **_kwargs) -> None:
        self._log.published.append(topic)

    async def subscribe(self, topic, **_kwargs) -> None:
        self._log.subscribed.append(topic)

    @property
    def messages(self):
        async def _empty():
            for message in ():
                yield message

        return _empty()


@contextmanager
def _fake_broker():
    """Replace the aiomqtt client used by both ``bus`` and ``cli``.

    Both modules do a plain ``import aiomqtt`` and reach through the
    module object, so this single patch also covers doctor's inline
    reachability probe in ``swarmbus.cli``. The doctor tests assert on
    the number of connections opened, which is what keeps that
    assumption honest if the imports ever change shape.
    """
    log = _BrokerLog()

    def _factory(*args, **kwargs):
        return _RecordingClient(log, *args, **kwargs)

    with patch("swarmbus.bus.aiomqtt.Client", side_effect=_factory):
        yield log


@contextmanager
def _probe_spy():
    """Record every bus ``AgentBus.probe`` really builds.

    A spy, not a stub: it delegates to the real classmethod and returns
    the real instance, so callers assert against the topic map the real
    ``TopicMap`` resolved.
    """
    real_probe = AgentBus.probe
    built: list[AgentBus] = []

    def _record(**kwargs):
        bus = real_probe(**kwargs)
        built.append(bus)
        return bus

    with patch.object(AgentBus, "probe", side_effect=_record):
        yield built


#: systemd is an external resource; doctor's unit check is not under
#: test here, and faking it keeps the run hermetic and fast.
_NO_SYSTEMD_UNIT = subprocess.CompletedProcess(
    args=[], returncode=1, stdout="", stderr=""
)


def _invoke_doctor(extra_args, env=None):
    """Run ``doctor`` against the fake broker. Returns (result, probes, log)."""
    runner = CliRunner()
    with _fake_broker() as log, _probe_spy() as probes, \
            patch("subprocess.run", return_value=_NO_SYSTEMD_UNIT):
        result = runner.invoke(
            main,
            ["doctor", "--agent-id", "doc-agent", *extra_args],
            env=env,
        )
    return result, probes, log


# ---------------------------------------------------------------------------
# send
# ---------------------------------------------------------------------------


def test_send_topic_root_reaches_the_published_inbox_topic():
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, [
            "send", "--agent-id", "tx", "--to", "rx",
            "--subject", "s", "--body", "b",
            "--topic-root", ROOT,
        ])
    assert result.exit_code == 0, result.output
    assert log.published == [rooted.inbox("rx")]
    assert log.published[0].startswith(f"{ROOT}/")


def test_send_default_publishes_the_unrooted_inbox_topic():
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, [
            "send", "--agent-id", "tx", "--to", "rx",
            "--subject", "s", "--body", "b",
        ])
    assert result.exit_code == 0, result.output
    assert log.published == [TopicMap().inbox("rx")]
    # Pinned literal: an unrooted invocation must keep speaking exactly
    # what it spoke before --topic-root existed.
    assert log.published == ["agents/rx/inbox"]


def test_send_broadcast_stays_unrooted_under_a_topic_root():
    """Broadcast is bus-wide by design and must ignore the root.

    Rooting it would split fan-out per namespace, which is the opposite
    of what broadcast is for. See the topics module docstring.
    """
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, [
            "send", "--agent-id", "tx", "--to", "broadcast",
            "--subject", "s", "--body", "b",
            "--topic-root", ROOT,
        ])
    assert result.exit_code == 0, result.output
    assert log.published == [rooted.broadcast]
    assert not log.published[0].startswith(f"{ROOT}/")


def test_send_picks_up_swarmbus_topic_root_env_var():
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(
            main,
            ["send", "--agent-id", "tx", "--to", "rx",
             "--subject", "s", "--body", "b"],
            env={"SWARMBUS_TOPIC_ROOT": ROOT},
        )
    assert result.exit_code == 0, result.output
    assert log.published == [rooted.inbox("rx")]


def test_send_topic_root_flag_beats_the_env_var():
    """Click contract: an explicit flag wins over the envvar fallback."""
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(
            main,
            ["send", "--agent-id", "tx", "--to", "rx",
             "--subject", "s", "--body", "b",
             "--topic-root", ROOT],
            env={"SWARMBUS_TOPIC_ROOT": "from-env"},
        )
    assert result.exit_code == 0, result.output
    assert log.published == [rooted.inbox("rx")]
    assert "from-env" not in log.published[0]


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


def test_start_topic_root_reaches_subscriptions_presence_and_lwt():
    """The LWT matters as much as the subscriptions here.

    An unrooted last-will on a rooted broker means nobody ever sees this
    agent go offline, and its retained ``online`` presence outlives it.
    """
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, [
            "start", "--agent-id", "rx", "--topic-root", ROOT,
        ])
    assert result.exit_code == 0, result.output
    assert log.subscribed == [rooted.inbox("rx"), rooted.broadcast]
    assert log.published == [rooted.presence("rx")]
    assert log.wills == [rooted.presence("rx")]


def test_start_default_uses_the_unrooted_topics():
    plain = TopicMap()
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, ["start", "--agent-id", "rx"])
    assert result.exit_code == 0, result.output
    assert log.subscribed == [plain.inbox("rx"), plain.broadcast]
    assert log.published == [plain.presence("rx")]
    assert log.wills == [plain.presence("rx")]


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


def test_read_topic_root_reaches_the_inbox_subscription():
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, [
            "read", "--agent-id", "rx", "--topic-root", ROOT,
        ])
    assert result.exit_code == 0, result.output
    assert log.subscribed == [rooted.inbox("rx")]
    assert log.subscribed[0].startswith(f"{ROOT}/")


def test_read_default_subscribes_to_the_unrooted_inbox():
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, ["read", "--agent-id", "rx"])
    assert result.exit_code == 0, result.output
    assert log.subscribed == [TopicMap().inbox("rx")]


# ---------------------------------------------------------------------------
# watch
# ---------------------------------------------------------------------------


def test_watch_topic_root_reaches_the_inbox_subscription():
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, [
            "watch", "--agent-id", "rx", "--timeout", "0.1",
            "--topic-root", ROOT,
        ])
    # Exit 1 is the no-message path: the fake broker delivers nothing.
    assert result.exit_code == 1, result.output
    assert log.subscribed == [rooted.inbox("rx")]
    assert log.subscribed[0].startswith(f"{ROOT}/")


def test_watch_default_subscribes_to_the_unrooted_inbox():
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, [
            "watch", "--agent-id", "rx", "--timeout", "0.1",
        ])
    assert result.exit_code == 1, result.output
    assert log.subscribed == [TopicMap().inbox("rx")]


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_topic_root_reaches_the_presence_filter():
    """`list` is the loudest symptom of a missed root: empty output."""
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, ["list", "--topic-root", ROOT])
    assert result.exit_code == 0, result.output
    assert log.subscribed == [rooted.any_presence_filter()]
    assert log.subscribed[0].startswith(f"{ROOT}/")


def test_list_default_uses_the_unrooted_presence_filter():
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(main, ["list"])
    assert result.exit_code == 0, result.output
    assert log.subscribed == [TopicMap().any_presence_filter()]
    # Pinned literal: unrooted discovery keeps its historical filter.
    assert log.subscribed == ["agents/+/presence"]


def test_list_picks_up_swarmbus_topic_root_env_var():
    """Covers the probe() path for the envvar, not just __init__."""
    rooted = TopicMap(root=ROOT)
    runner = CliRunner()
    with _fake_broker() as log:
        result = runner.invoke(
            main, ["list"], env={"SWARMBUS_TOPIC_ROOT": ROOT}
        )
    assert result.exit_code == 0, result.output
    assert log.subscribed == [rooted.any_presence_filter()]


# ---------------------------------------------------------------------------
# doctor -- one option, two call sites, ~160 lines apart
# ---------------------------------------------------------------------------


def test_doctor_roots_both_probes_independently_broker_check_and_peer_discovery():
    """doctor's single --topic-root must reach BOTH of its probes.

    Check 2 (broker reachability) and check 7 (peer discovery) each build
    their own ``AgentBus.probe``, separated by ~160 lines of unrelated
    checks. That is the exact shape where someone wires the first and
    misses the second: the run still goes green, because an unrooted
    peer-discovery probe just reports zero peers, which reads as "the
    daemon is not announcing" rather than "doctor is looking in the
    wrong namespace".

    Check 2's probe is asserted on the constructed object because that
    probe is only ever used for its ``_aiomqtt_kwargs()`` -- its topic
    map never reaches the wire, so there is nothing else to observe.
    Check 7 is asserted on the real subscription string as well.
    """
    rooted = TopicMap(root=ROOT)
    result, probes, log = _invoke_doctor(["--topic-root", ROOT])

    # Exit 2 means doctor bailed before running its checks.
    assert result.exit_code in (0, 1), result.output
    assert len(probes) == 2, (
        f"expected doctor to build 2 probes (checks 2 and 7), got "
        f"{len(probes)}:\n{result.output}"
    )
    broker_check_probe, peer_discovery_probe = probes

    assert broker_check_probe.topics == rooted, (
        "doctor check 2 (broker reachability) built an unrooted probe"
    )
    assert peer_discovery_probe.topics == rooted, (
        "doctor check 7 (peer discovery) built an unrooted probe"
    )

    # Check 7 is the one that actually talks: its subscription is the
    # string an operator's missing peers hinge on.
    assert log.subscribed == [rooted.any_presence_filter()]
    assert log.subscribed[0].startswith(f"{ROOT}/")

    # Two broker connections: check 2's reachability ping and check 7's
    # presence sweep. Also confirms the single aiomqtt patch really does
    # cover the cli module's own call site.
    assert len(log.clients) == 2, result.output


def test_doctor_default_leaves_both_probes_unrooted():
    plain = TopicMap()
    result, probes, log = _invoke_doctor([])

    assert result.exit_code in (0, 1), result.output
    assert len(probes) == 2, result.output
    assert [p.topics for p in probes] == [plain, plain]
    assert log.subscribed == [plain.any_presence_filter()]


# ---------------------------------------------------------------------------
# structural guards -- these are what catch the NEXT command
# ---------------------------------------------------------------------------


def _reference_topic_root_option() -> click.Option:
    """The option the shared decorator itself produces.

    Expectations come from the producer rather than from literals typed
    into this file: if the decorator's envvar or default changes, every
    command has to change with it, and a hand-rolled look-alike bolted
    onto some future command fails the comparison.
    """

    def _carrier() -> None:
        pass

    decorated = _topic_root_option(_carrier)
    params = getattr(decorated, "__click_params__", [])
    assert len(params) == 1, (
        "_topic_root_option should contribute exactly one Click option"
    )
    return params[0]


def _bus_construction_sites() -> list[tuple[str, ast.Call]]:
    """Every ``AgentBus(...)`` / ``AgentBus.probe(...)`` call in the CLI.

    Returned as (enclosing top-level function name, call node). Nested
    helpers are attributed to the top-level function that owns them,
    which is the Click callback.
    """
    tree = ast.parse(inspect.getsource(swarmbus.cli))
    sites: list[tuple[str, ast.Call]] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue
            func = child.func
            is_ctor = isinstance(func, ast.Name) and func.id == "AgentBus"
            is_probe = (
                isinstance(func, ast.Attribute)
                and func.attr == "probe"
                and isinstance(func.value, ast.Name)
                and func.value.id == "AgentBus"
            )
            if is_ctor or is_probe:
                sites.append((node.name, child))
    return sites


def test_every_cli_command_that_builds_a_bus_carries_the_shared_topic_root_option():
    """The guard against a future command being added without the root.

    Enumerates the module's Click commands, keeps the ones whose
    callback constructs a bus, and requires each to carry the very
    option ``_topic_root_option`` hands out. Adding a seventh
    broker-facing command without ``@_topic_root_option`` fails here
    with no test edit needed -- that is the whole point of this one.
    """
    reference = _reference_topic_root_option()
    builders = {name for name, _call in _bus_construction_sites()}
    covered: set[str] = set()

    for name, command in main.commands.items():
        callback = command.callback
        if callback is None or callback.__name__ not in builders:
            continue
        covered.add(name)
        matching = [p for p in command.params if p.name == reference.name]
        assert matching, (
            f"command {name!r} builds an AgentBus but has no "
            f"{reference.name!r} parameter -- apply @_topic_root_option"
        )
        option = matching[0]
        assert option.opts == reference.opts, name
        assert option.envvar == reference.envvar, name
        assert option.default == reference.default, name

    # Without this, a detector that matched nothing would pass silently.
    assert covered >= KNOWN_BUS_COMMANDS, (
        f"expected at least {sorted(KNOWN_BUS_COMMANDS)} to be detected as "
        f"bus builders, found {sorted(covered)}"
    )


def test_every_cli_command_with_a_topic_root_param_matches_the_shared_option():
    """No divergent hand-rolled copies of the option.

    ``mcp-server`` declares its own ``--topic-root`` inline rather than
    using the decorator (it hands the value to ``run_mcp_server``, not to
    an ``AgentBus``, so the test above does not see it). A second
    declaration is a second place for the envvar name or the default to
    drift, and drift here is invisible: the flag still parses, it just
    stops agreeing with every other process on the broker.
    """
    reference = _reference_topic_root_option()
    seen: set[str] = set()

    for name, command in main.commands.items():
        matching = [p for p in command.params if p.name == reference.name]
        if not matching:
            continue
        seen.add(name)
        option = matching[0]
        assert option.opts == reference.opts, name
        assert option.envvar == reference.envvar, name
        assert option.default == reference.default, name

    assert seen >= KNOWN_BUS_COMMANDS | {"mcp-server"}, (
        f"expected every broker-facing command to declare a topic root, "
        f"found {sorted(seen)}"
    )


def test_every_agentbus_construction_site_in_cli_passes_topic_root():
    """Per call site, not per command.

    A command can carry the option and still leave one of its own bus
    constructions unrooted -- doctor has two. This asserts on the actual
    call nodes so an unrooted site fails even if its command's option is
    wired correctly.
    """
    sites = _bus_construction_sites()
    assert sites, (
        "found no AgentBus construction in swarmbus.cli -- the detector "
        "broke, or the class was renamed"
    )
    unrooted = [
        f"{owner}() at line {call.lineno}"
        for owner, call in sites
        if "topic_root" not in {kw.arg for kw in call.keywords}
    ]
    assert not unrooted, (
        "AgentBus built without topic_root=: " + ", ".join(unrooted)
    )


def test_doctor_still_has_exactly_two_bus_sites_so_the_both_probes_test_stays_complete():
    """Tripwire for the doctor test above.

    ``test_doctor_roots_both_probes_independently_...`` unpacks exactly
    two probes. If doctor grows a third bus, that test would need
    updating to assert on it too -- fail here and say so rather than
    let the new site go uncovered.
    """
    doctor_sites = [
        call for owner, call in _bus_construction_sites() if owner == "doctor"
    ]
    assert len(doctor_sites) == 2, (
        f"doctor now builds {len(doctor_sites)} buses, not 2 -- extend "
        f"test_doctor_roots_both_probes_independently_broker_check_and_"
        f"peer_discovery to cover every one of them"
    )
