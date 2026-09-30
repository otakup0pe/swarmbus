"""Tests for swarmbus init command and helpers."""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from swarmbus.cli import _derive_invoke, _run_step, main


# ---------------------------------------------------------------------------
# _run_step unit tests
# ---------------------------------------------------------------------------

class TestRunStep:
    def test_dry_run_no_subprocess(self):
        with patch("subprocess.run") as mock_run:
            result = _run_step("Test step", ["echo", "hello"], dry_run=True)
        assert result is True
        mock_run.assert_not_called()

    def test_dry_run_prints_command(self, capsys):
        _run_step("Test step", ["echo", "hello"], dry_run=True)
        captured = capsys.readouterr()
        assert "would run: echo hello" in captured.out

    def test_real_success(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""
        with patch("subprocess.run", return_value=mock_result):
            result = _run_step("Test step", ["true"], dry_run=False)
        assert result is True

    def test_real_failure_returns_false(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "some stdout output"
        mock_result.stderr = "some stderr output"
        with patch("subprocess.run", return_value=mock_result):
            result = _run_step("Test step", ["false"], dry_run=False)
        assert result is False

    def test_real_failure_prints_stdout_and_stderr(self, capsys):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "script printed this"
        mock_result.stderr = "error detail"
        with patch("subprocess.run", return_value=mock_result):
            _run_step("Test step", ["false"], dry_run=False)
        captured = capsys.readouterr()
        assert "script printed this" in captured.out
        assert "error detail" in captured.out


# ---------------------------------------------------------------------------
# _derive_invoke
# ---------------------------------------------------------------------------

class TestDeriveInvoke:
    def test_cc_with_repo_root(self):
        result = _derive_invoke("cc", "sparrow", "/home/user/swarmbus")
        assert result == "/home/user/swarmbus/examples/claude-code-wake.sh sparrow"

    def test_openclaw_with_repo_root(self):
        result = _derive_invoke("openclaw", "wren", "/home/user/swarmbus")
        assert result == "/home/user/swarmbus/examples/openclaw-wake.sh wren"

    def test_none_host_type_returns_none(self):
        result = _derive_invoke("none", "test", "/home/user/swarmbus")
        assert result is None

    def test_no_repo_root_returns_none(self):
        result = _derive_invoke("cc", "sparrow", None)
        assert result is None

    def test_no_repo_root_openclaw_returns_none(self):
        result = _derive_invoke("openclaw", "wren", None)
        assert result is None


# ---------------------------------------------------------------------------
# CLI flag validation
# ---------------------------------------------------------------------------

class TestAgentIdValidation:
    def _invoke(self, agent_id: str) -> int:
        runner = CliRunner()
        result = runner.invoke(main, [
            "init", "--agent-id", agent_id, "--dry-run", "--skip-broker",
        ])
        return result.exit_code

    def test_valid_lowercase(self):
        with patch("swarmbus.cli.resolve_broker_addr", return_value="localhost"), \
             patch("swarmbus.cli.detect_platform", return_value="debian"), \
             patch("swarmbus.cli.find_repo_root", return_value=None):
            assert self._invoke("sparrow") == 0

    def test_valid_with_hyphen(self):
        with patch("swarmbus.cli.resolve_broker_addr", return_value="localhost"), \
             patch("swarmbus.cli.detect_platform", return_value="debian"), \
             patch("swarmbus.cli.find_repo_root", return_value=None):
            assert self._invoke("wren-beta") == 0

    def test_valid_with_underscore(self):
        with patch("swarmbus.cli.resolve_broker_addr", return_value="localhost"), \
             patch("swarmbus.cli.detect_platform", return_value="debian"), \
             patch("swarmbus.cli.find_repo_root", return_value=None):
            assert self._invoke("agent_01") == 0

    def test_invalid_uppercase(self):
        assert self._invoke("Sparrow") == 2

    def test_invalid_special_chars(self):
        assert self._invoke("sparrow!") == 2

    def test_invalid_with_space(self):
        assert self._invoke("my agent") == 2

    def test_invalid_with_slash(self):
        assert self._invoke("foo/bar") == 2

    def test_reserved_broadcast(self):
        assert self._invoke("broadcast") == 2

    def test_reserved_system(self):
        assert self._invoke("system") == 2


class TestHostTypeChoices:
    def test_cc_accepted(self):
        runner = CliRunner()
        with patch("swarmbus.cli.resolve_broker_addr", return_value="localhost"), \
             patch("swarmbus.cli.detect_platform", return_value="debian"), \
             patch("swarmbus.cli.find_repo_root", return_value=None):
            result = runner.invoke(main, ["init", "--agent-id", "test", "--host-type", "cc", "--dry-run", "--skip-broker"])
        assert result.exit_code == 0

    def test_openclaw_accepted(self):
        runner = CliRunner()
        with patch("swarmbus.cli.resolve_broker_addr", return_value="localhost"), \
             patch("swarmbus.cli.detect_platform", return_value="debian"), \
             patch("swarmbus.cli.find_repo_root", return_value=None):
            result = runner.invoke(main, ["init", "--agent-id", "test", "--host-type", "openclaw", "--dry-run", "--skip-broker"])
        assert result.exit_code == 0

    def test_none_accepted(self):
        runner = CliRunner()
        with patch("swarmbus.cli.resolve_broker_addr", return_value="localhost"), \
             patch("swarmbus.cli.detect_platform", return_value="debian"), \
             patch("swarmbus.cli.find_repo_root", return_value=None):
            result = runner.invoke(main, ["init", "--agent-id", "test", "--host-type", "none", "--dry-run", "--skip-broker"])
        assert result.exit_code == 0

    def test_invalid_choice_rejected(self):
        runner = CliRunner()
        result = runner.invoke(main, ["init", "--agent-id", "test", "--host-type", "docker", "--dry-run"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output


class TestHelpShowsAllFlags:
    def test_help_exit_0(self):
        runner = CliRunner()
        result = runner.invoke(main, ["init", "--help"])
        assert result.exit_code == 0

    def test_help_shows_flags(self):
        runner = CliRunner()
        result = runner.invoke(main, ["init", "--help"])
        for flag in ["--agent-id", "--host-type", "--broker", "--invoke",
                     "--inbox", "--skip-broker", "--skip-plugin", "--dry-run", "--yes"]:
            assert flag in result.output, f"Missing {flag} in --help output"


# ---------------------------------------------------------------------------
# Dry-run smoke tests
# ---------------------------------------------------------------------------

def _patch_platform(repo_root="/repo"):
    """Return a context stack that patches platform detection."""
    from contextlib import ExitStack
    stack = ExitStack()
    stack.enter_context(patch("swarmbus.cli.resolve_broker_addr", return_value="localhost"))
    stack.enter_context(patch("swarmbus.cli.detect_platform", return_value="debian"))
    stack.enter_context(patch("swarmbus.cli.find_repo_root", return_value=repo_root))
    return stack


class TestDryRunSmoke:
    def test_dry_run_cc_exits_0(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "cc", "--dry-run",
            ])
        assert result.exit_code == 0, result.output

    def test_dry_run_cc_prints_six_steps(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "cc", "--dry-run",
            ])
        for step in ["Broker", "Package", "Systemd unit", "Wake wrapper", "Host plugin", "Doctor"]:
            assert step in result.output, f"Missing step '{step}' in output"

    def test_dry_run_none_host_exits_0(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "none", "--dry-run",
            ])
        assert result.exit_code == 0, result.output

    def test_dry_run_none_host_plugin_skipped(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "none", "--dry-run",
            ])
        assert "skipped" in result.output

    def test_dry_run_skip_broker(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--dry-run", "--skip-broker",
            ])
        assert result.exit_code == 0, result.output
        assert "--skip-broker" in result.output or "skipped" in result.output

    def test_dry_run_skip_plugin(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "cc", "--dry-run", "--skip-plugin",
            ])
        assert result.exit_code == 0, result.output

    def test_dry_run_shows_success_banner(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "sparrow", "--dry-run",
            ])
        assert "sparrow is ready" in result.output


# ---------------------------------------------------------------------------
# Wake wrapper fallback (no repo root)
# ---------------------------------------------------------------------------

class TestWakeWrapperFallback:
    def test_no_repo_root_warns_not_fails(self):
        runner = CliRunner()
        with _patch_platform(repo_root=None):
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "cc", "--dry-run",
            ])
        assert result.exit_code == 0, result.output
        assert "⚠" in result.output or "not wired" in result.output

    def test_invoke_path_missing_warns_not_fails(self, tmp_path):
        runner = CliRunner()
        nonexistent = str(tmp_path / "nonexistent-wake.sh")
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "cc",
                "--invoke", f"{nonexistent} test",
                "--dry-run",
            ])
        assert result.exit_code == 0, result.output

    def test_invoke_override_used_when_supplied(self):
        runner = CliRunner()
        with _patch_platform():
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "none",
                "--invoke", "/custom/wake.sh test",
                "--dry-run",
            ])
        assert "/custom/wake.sh test" in result.output


# ---------------------------------------------------------------------------
# Plugin step positional args
# ---------------------------------------------------------------------------

class TestPluginStepArgs:
    def test_cc_plugin_uses_positional_args(self):
        runner = CliRunner()
        captured_cmds = []

        def fake_run_step(label, cmd, dry_run):
            captured_cmds.append(cmd)
            return True

        with _patch_platform(), \
             patch("swarmbus.cli._run_step", side_effect=fake_run_step):
            runner.invoke(main, [
                "init", "--agent-id", "sparrow", "--host-type", "cc",
                "--skip-broker",
            ])

        plugin_calls = [cmd for cmd in captured_cmds if "setup-cc-plugin" in " ".join(cmd)]
        assert len(plugin_calls) == 1
        cmd = plugin_calls[0]
        # Positional args: <agent-id> <broker> — NOT flags
        assert "sparrow" in cmd
        assert "localhost" in cmd
        assert "--agent-id" not in cmd  # not using flags

    def test_none_host_type_no_plugin_call(self):
        runner = CliRunner()
        captured_cmds = []

        def fake_run_step(label, cmd, dry_run):
            captured_cmds.append(cmd)
            return True

        with _patch_platform(), \
             patch("swarmbus.cli._run_step", side_effect=fake_run_step):
            runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "none",
                "--skip-broker",
            ])

        plugin_calls = [cmd for cmd in captured_cmds if "setup-cc-plugin" in " ".join(cmd)
                        or "setup-openclaw-plugin" in " ".join(cmd)]
        assert len(plugin_calls) == 0


# ---------------------------------------------------------------------------
# Exit code tests
# ---------------------------------------------------------------------------

class TestExitCodes:
    def test_all_steps_pass_exits_0(self):
        runner = CliRunner()
        with _patch_platform(), \
             patch("swarmbus.cli._run_step", return_value=True):
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "none", "--skip-broker",
            ])
        assert result.exit_code == 0, result.output

    def test_broker_failure_exits_1(self):
        runner = CliRunner()
        call_count = [0]

        def fake_run_step(label, cmd, dry_run):
            call_count[0] += 1
            # broker is first real step call
            if call_count[0] == 1:
                return False
            return True

        with _patch_platform(), \
             patch("swarmbus.cli._run_step", side_effect=fake_run_step):
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--host-type", "none",
            ])
        assert result.exit_code == 1

    def test_partial_failure_exits_1(self):
        runner = CliRunner()

        def fake_run_step(label, cmd, dry_run):
            # doctor step fails
            if "doctor" in " ".join(cmd):
                return False
            return True

        with _patch_platform(), \
             patch("swarmbus.cli._run_step", side_effect=fake_run_step):
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--skip-broker",
            ])
        assert result.exit_code == 1

    def test_broker_resolve_error_exits_1(self):
        runner = CliRunner()
        with patch("swarmbus.cli.resolve_broker_addr",
                   side_effect=RuntimeError("tailscale not found")):
            result = runner.invoke(main, [
                "init", "--agent-id", "test", "--broker", "tailscale",
            ])
        assert result.exit_code == 1


class TestStepDoctor:
    """Regression: _step_doctor must build a valid subprocess list even when
    swarmbus is not on PATH (the old fallback produced a single string like
    'python3 -m swarmbus' as a list element, which subprocess.run passes
    verbatim as the executable name and raises FileNotFoundError)."""

    def test_doctor_uses_swarmbus_on_path(self):
        from swarmbus.cli import _step_doctor
        captured = []

        def fake_run_step(label, cmd, dry_run):
            captured.append(cmd)
            return True

        with patch("swarmbus.cli.shutil.which", return_value="/usr/bin/swarmbus"), \
             patch("swarmbus.cli._run_step", side_effect=fake_run_step):
            _step_doctor("sparrow", dry_run=True)

        assert captured[0][0] == "/usr/bin/swarmbus"
        assert "--agent-id" in captured[0]

    def test_doctor_fallback_is_list_not_string(self):
        """When swarmbus not on PATH, cmd must be a proper list of strings."""
        from swarmbus.cli import _step_doctor
        captured = []

        def fake_run_step(label, cmd, dry_run):
            captured.append(cmd)
            return True

        with patch("swarmbus.cli.shutil.which", return_value=None), \
             patch("swarmbus.cli._run_step", side_effect=fake_run_step):
            _step_doctor("sparrow", dry_run=True)

        cmd = captured[0]
        # Must be a proper list — no element should contain spaces
        for element in cmd:
            assert " " not in element, f"Element {element!r} contains space — subprocess would fail"
        assert "doctor" in cmd


# ---------------------------------------------------------------------------
# --topic-root propagation
# ---------------------------------------------------------------------------

#: The namespace a rooted deployment would use. Test input.
TOPIC_ROOT = "loom"


def _init_step_argvs(*extra_args, env=None):
    """Run ``init`` and return every argv it handed to ``_run_step``.

    ``_run_step`` is the single seam every subprocess init spawns passes
    through, so capturing there sees the literal argv -- which is the
    thing under test. Everything upstream of it (option parsing, the
    step functions that build the commands) is the real implementation.

    ``--host-type none`` and ``--skip-broker`` trim the run to the two
    steps that matter here: the systemd installer and the doctor
    subprocess.
    """
    captured: list[list[str]] = []

    def fake_run_step(label, cmd, dry_run):
        captured.append(cmd)
        return True

    with _patch_platform(), \
         patch("swarmbus.cli.shutil.which", return_value="/usr/bin/swarmbus"), \
         patch("swarmbus.cli._run_step", side_effect=fake_run_step):
        result = CliRunner().invoke(main, [
            "init", "--agent-id", "rx", "--host-type", "none",
            "--skip-broker", "--dry-run", *extra_args,
        ], env=env)
    assert result.exit_code == 0, result.output
    return captured


def _only_argv_containing(argvs, needle):
    """The single captured argv whose command contains ``needle``."""
    matching = [cmd for cmd in argvs if needle in " ".join(cmd)]
    assert len(matching) == 1, (
        f"expected exactly one init step running {needle!r}, got {matching}"
    )
    return matching[0]


class TestTopicRootPropagation:
    """``swarmbus init --topic-root`` must reach BOTH subprocesses.

    Two call sites, and missing either one is silent. ``install-systemd.sh``
    writes the root into a ``topic.conf`` drop-in; without it the daemon
    init just installed comes up unrooted and never hears anyone, with no
    error at either end. And init runs ``doctor`` as a SUBPROCESS, so the
    root it just installed lives in that drop-in and not in this process's
    environment -- an unforwarded doctor probes the unrooted namespace and
    cheerfully reports a healthy broker while the new daemon is talking
    somewhere else.
    """

    def test_topic_root_reaches_the_install_systemd_argv(self):
        argvs = _init_step_argvs("--topic-root", TOPIC_ROOT)
        cmd = _only_argv_containing(argvs, "install-systemd.sh")
        assert "--topic-root" in cmd, cmd
        assert cmd[cmd.index("--topic-root") + 1] == TOPIC_ROOT, cmd

    def test_topic_root_reaches_the_doctor_subprocess_argv(self):
        argvs = _init_step_argvs("--topic-root", TOPIC_ROOT)
        cmd = _only_argv_containing(argvs, "doctor")
        assert "--topic-root" in cmd, cmd
        assert cmd[cmd.index("--topic-root") + 1] == TOPIC_ROOT, cmd

    def test_topic_root_from_the_env_var_reaches_both_argvs(self):
        """The exported-in-a-shell case, which is the common one.

        systemd does not inherit an operator's interactive environment,
        so an exported root that init fails to forward configures every
        interactive command and reaches the installed daemon not at all.
        """
        argvs = _init_step_argvs(env={"SWARMBUS_TOPIC_ROOT": TOPIC_ROOT})
        for needle in ("install-systemd.sh", "doctor"):
            cmd = _only_argv_containing(argvs, needle)
            assert "--topic-root" in cmd, (needle, cmd)
            assert cmd[cmd.index("--topic-root") + 1] == TOPIC_ROOT, (needle, cmd)

    def test_unset_topic_root_omits_the_flag_from_both_argvs(self, monkeypatch):
        """Absence of the flag, not an empty value.

        ``--topic-root ''`` is not the same thing as no flag:
        install-systemd.sh would still take the branch and doctor would
        still parse an option. The unrooted default has to leave both
        argvs byte-identical to what they were before the option existed.
        """
        monkeypatch.delenv("SWARMBUS_TOPIC_ROOT", raising=False)
        argvs = _init_step_argvs()
        for needle in ("install-systemd.sh", "doctor"):
            cmd = _only_argv_containing(argvs, needle)
            assert "--topic-root" not in cmd, (needle, cmd)
            assert not any("topic-root" in element for element in cmd), (needle, cmd)
