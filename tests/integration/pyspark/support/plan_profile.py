"""Opt-in query plan profiling; explain time is not executor time."""

from contextlib import redirect_stdout
from io import StringIO
from time import perf_counter


def profile_checkpoints(monkeypatch, frame_type) -> None:
    if not hasattr(frame_type, "checkpoint"):
        return
    original = frame_type.checkpoint

    def checkpoint(frame, eager=True):
        started = perf_counter()
        with redirect_stdout(StringIO()) as output:
            frame.explain(mode="simple")
        planning = perf_counter() - started
        print(f"[plan-profile] checkpoint explain: {planning:.3f}s; plan characters={len(output.getvalue())}")
        started = perf_counter()
        try:
            return original(frame, eager=eager)
        finally:
            print(f"[plan-profile] checkpoint after explain (eager={eager}): {perf_counter() - started:.3f}s")

    monkeypatch.setattr(frame_type, "checkpoint", checkpoint)


def profile_guards(monkeypatch) -> None:
    import importlib

    from structure.plugin.pyspark.execution.logic.running.RunOnlinePySparkTransform import RunOnlinePySparkTransform

    for name in ("_exactly_one", "_require_all", "_require_unique"):
        original = getattr(RunOnlinePySparkTransform, name)
        monkeypatch.setattr(RunOnlinePySparkTransform, name, timed_guard(original, f"online {name}"))
    policies = importlib.import_module("structure.plugin.pyspark.execution.logic.PolicyChecks")
    monkeypatch.setattr(policies, "check_policy", timed_guard(policies.check_policy, "singleton policy (cache miss)"))
    original_run = RunOnlinePySparkTransform._run

    def run(executor, invocation, plan, *, session):
        for step, operation, before, after in guard_expansions(plan):
            print(f"[plan-profile] {step} {operation}: expanded input references {before} -> {after} (estimate)")
        return original_run(executor, invocation, plan, session=session)

    monkeypatch.setattr(RunOnlinePySparkTransform, "_run", run)


def guard_expansions(plan):
    """Estimate source copies, not rows or Catalyst nodes; views do not reset the count."""
    frames = {name: 1 for item in plan.inputs for name in (item.name, f"input:{item.name}")}
    findings = []
    for step in plan.steps:
        size = frames.get(step.source, 1)
        for operation in step.operations:
            before = size
            if operation.join is not None:
                right = frames.get(operation.join.source, 1)
                if operation.join.assert_singleton_in_batch:
                    findings.append((step.name, "singleton (old -> bounded source copies)", right * 2, right))
                size += right
            elif operation.relation_set is not None:
                size += frames.get(operation.relation_set.source, 1)
            elif operation.kind in {"require_all", "require_unique"}:
                size *= 2
                findings.append((step.name, operation.kind, before, size))
            elif operation.kind in {"checkpoint", "local_checkpoint"}:
                size = 1
                findings.append((step.name, operation.kind, before, size))
        frames.update({result.frame: size for result in step.results})
    return findings


def timed_guard(original, name):
    def run(*args, **kwargs):
        started = perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            print(f"[plan-profile] {name}: {perf_counter() - started:.3f}s")

    return run
