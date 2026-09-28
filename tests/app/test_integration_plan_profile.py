from types import SimpleNamespace as Record

import pytest
from integration.pyspark.support.plan_profile import guard_expansions, profile_checkpoints, profile_mode


class Frame:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def explain(self, mode):
        self.calls.append(("explain", mode))
        print("Small physical query plan")

    def checkpoint(self, eager=True):
        self.calls.append(("checkpoint", eager))
        if self.fail:
            raise ValueError("checkpoint failed")
        return self


def test_checkpoint_profile_separates_explain_and_remaining_work(monkeypatch, capsys):
    frame = Frame()
    profile_checkpoints(monkeypatch, Frame)
    assert frame.checkpoint(eager=False) is frame
    assert frame.calls == [("explain", "simple"), ("checkpoint", False)]
    output = capsys.readouterr().out
    assert "checkpoint #1 (execution_mode=unknown, step=unmapped) explain:" in output
    assert "checkpoint #1 (execution_mode=unknown, step=unmapped) after explain (eager=False, outcome=success):" in output
    assert "Small physical query plan" not in output


def test_checkpoint_profile_reports_failed_attempt_and_restores_methods(monkeypatch, capsys):
    original = Frame.checkpoint
    with monkeypatch.context() as scoped:
        profile_checkpoints(scoped, Frame)
        with pytest.raises(ValueError, match="checkpoint failed"):
            Frame(fail=True).checkpoint()
    assert Frame.checkpoint is original
    assert "outcome=failure" in capsys.readouterr().out


def test_checkpoint_timing_does_not_explain_and_preserves_result_and_eager(monkeypatch, capsys):
    frame = Frame()
    profile_checkpoints(monkeypatch, Frame, explain=False)

    assert frame.checkpoint(eager=False) is frame

    assert frame.calls == [("checkpoint", False)]
    output = capsys.readouterr().out
    assert "explain" not in output
    assert "checkpoint #1 (execution_mode=unknown, step=unmapped) timing (eager=False, outcome=success):" in output


def test_profile_mode_supports_aliases_and_rejects_unknown_values():
    assert profile_mode(None) == "off"
    assert profile_mode("") == "off"
    assert profile_mode("0") == "off"
    assert profile_mode("off") == "off"
    assert profile_mode("timing") == "timing"
    assert profile_mode("1") == "explain"
    assert profile_mode("explain") == "explain"
    with pytest.raises(ValueError, match="one of off, timing, explain, or 1"):
        profile_mode("unexpected")


def test_guard_expansion_estimate_tracks_joins_assertions_and_checkpoints():
    operations = [
        Record(kind="join", join=Record(source="policy", assert_singleton_in_batch=True), relation_set=None),
        Record(kind="require_all", join=None, relation_set=None),
        Record(kind="checkpoint", join=None, relation_set=None),
        Record(kind="require_unique", join=None, relation_set=None),
    ]
    plan = Record(
        inputs=[Record(name="rows"), Record(name="policy")],
        steps=[Record(name="validate", source="rows", operations=operations, results=[Record(frame="result")])],
    )
    assert guard_expansions(plan) == [
        ("validate", "singleton (old -> bounded source copies)", 2, 1),
        ("validate", "require_all", 2, 4),
        ("validate", "checkpoint", 4, 1),
        ("validate", "require_unique", 1, 2),
    ]
