"""Contracts for the packaged using-swarmbus skill."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTING_RUNBOOK = (
    ROOT
    / "src"
    / "swarmbus"
    / "skills"
    / "using-swarmbus"
    / "references"
    / "testing-runbook.md"
)


def test_acceptance_runbook_makes_closeout_terminal():
    runbook = TESTING_RUNBOOK.read_text()

    assert "terminal protocol state" in runbook
    assert "must not be acknowledged" in runbook
    assert "consume it silently" in runbook
    assert "stops `watch_inbox` for the test" in runbook
