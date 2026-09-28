"""Opt-in query plan profiling; explain time is not executor time."""

from contextlib import redirect_stdout
from contextvars import ContextVar
from io import StringIO
from time import perf_counter
from typing import Any

_EXECUTION_MODE: ContextVar[str] = ContextVar("plan_profile_execution_mode", default="unknown")
_CHECKPOINT_STEPS: ContextVar[tuple[str, ...]] = ContextVar("plan_profile_checkpoint_steps", default=())
_CHECKPOINT_CURSOR: ContextVar[int] = ContextVar("plan_profile_checkpoint_cursor", default=0)


def _checkpoint_steps(plan: Any) -> tuple[str, ...]:
    return tuple(
        step.name
        for step in plan.steps
        for operation in step.operations
        if operation.kind in {"checkpoint", "local_checkpoint"}
    )


def profile_mode(value: str | None) -> str:
    """Normalize the integration-only profiler switch before Spark is requested."""

    if value in (None, "", "0", "off"):
        return "off"
    if value in ("1", "explain"):
        return "explain"
    if value == "timing":
        return "timing"
    raise ValueError(
        "STRUCTURE_PROFILE_QUERY_PLANS must be one of off, timing, explain, or 1; "
        f"got {value!r}"
    )


def profile_checkpoints(monkeypatch, frame_type, *, explain: bool = True) -> None:
    """Profile checkpoint calls without retaining frames or adding actions."""

    if not hasattr(frame_type, "checkpoint"):
        return
    original = frame_type.checkpoint
    sequence = 0

    def checkpoint(frame, eager=True):
        nonlocal sequence
        sequence += 1
        current = sequence
        context = _EXECUTION_MODE.get()
        steps = _CHECKPOINT_STEPS.get()
        cursor = _CHECKPOINT_CURSOR.get()
        step = steps[cursor] if cursor < len(steps) else "unmapped"
        _CHECKPOINT_CURSOR.set(cursor + 1)
        started = perf_counter()
        if explain:
            with redirect_stdout(StringIO()) as output:
                frame.explain(mode="simple")
            planning = perf_counter() - started
            print(
                f"[plan-profile] checkpoint #{current} (execution_mode={context}, step={step}) "
                f"explain: {planning:.3f}s; plan characters={len(output.getvalue())}"
            )
            started = perf_counter()
        succeeded = False
        try:
            result = original(frame, eager=eager)
            succeeded = True
            return result
        finally:
            suffix = "after explain" if explain else "timing"
            outcome = "success" if succeeded else "failure"
            print(
                f"[plan-profile] checkpoint #{current} (execution_mode={context}, step={step}) "
                f"{suffix} (eager={eager}, outcome={outcome}): {perf_counter() - started:.3f}s"
            )

    monkeypatch.setattr(frame_type, "checkpoint", checkpoint)

    try:
        from structure.core.runtime.session.model.StructureSession import StructureSession
    except ImportError:  # pragma: no cover - the unit test uses a stand-in frame only.
        return

    original_run = StructureSession.run

    def run(session, *args: Any, **kwargs: Any):
        mode_token = _EXECUTION_MODE.set(getattr(session, "execution_mode", "unknown"))
        steps_token = None
        cursor_token = None
        if args and not _CHECKPOINT_STEPS.get():
            try:
                artifact = session._compiled(args[0])
                steps = _checkpoint_steps(artifact.pyspark_plan)
            except Exception:
                steps = ()
            if steps:
                print(
                    "[plan-profile] checkpoint map: "
                    + ", ".join(f"#{index}={name}" for index, name in enumerate(steps, start=1))
                )
                steps_token = _CHECKPOINT_STEPS.set(steps)
                cursor_token = _CHECKPOINT_CURSOR.set(0)
        try:
            return original_run(session, *args, **kwargs)
        finally:
            if cursor_token is not None:
                _CHECKPOINT_CURSOR.reset(cursor_token)
            if steps_token is not None:
                _CHECKPOINT_STEPS.reset(steps_token)
            _EXECUTION_MODE.reset(mode_token)

    monkeypatch.setattr(StructureSession, "run", run)


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
        checkpoint_steps = _checkpoint_steps(plan)
        print(
            "[plan-profile] checkpoint map: "
            + (", ".join(f"#{index}={name}" for index, name in enumerate(checkpoint_steps, start=1)) or "none")
        )
        steps_token = _CHECKPOINT_STEPS.set(checkpoint_steps)
        cursor_token = _CHECKPOINT_CURSOR.set(0)
        try:
            return original_run(executor, invocation, plan, session=session)
        finally:
            _CHECKPOINT_CURSOR.reset(cursor_token)
            _CHECKPOINT_STEPS.reset(steps_token)

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
