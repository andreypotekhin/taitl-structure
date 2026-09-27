"""Lazy, bounded policy checks owned by one transform invocation."""

from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")
_checks: ContextVar[dict[tuple[int, str], tuple[Any, Any]] | None] = ContextVar("pyspark_policy_checks", default=None)


def reuse_policy_checks(run: Callable[P, R]) -> Callable[P, R]:
    @wraps(run)
    def scoped(*args: P.args, **kwargs: P.kwargs) -> R:
        token = _checks.set({})
        try:
            return run(*args, **kwargs)
        finally:
            _checks.reset(token)

    return scoped


def singleton_policy(frame, scope: str):
    if frame.isStreaming:
        return frame
    checks = _checks.get()
    key = (id(frame), scope)
    if checks is not None and key in checks:
        checked = checks[key][1]
    else:
        checked = check_policy(frame, scope)
        if checks is not None:
            # Retain the input as well: Python must not recycle its identity during this run.
            checks[key] = (frame, checked)
    # Each use needs fresh output attributes for repeated joins after a checkpoint (Spark 3.5).
    return checked.select("__structure_policy.*")


def check_policy(frame, scope: str):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    message = f"REL-E0701: exactly_one({scope}) requires exactly one row; see docs/Diagnostics.md#rel-e0701"
    # Two rows suffice to disprove singleton cardinality. This collection is bounded, even for large inputs.
    # Structs retain null-valued rows and the original fields' types/nullability/metadata.
    counted = frame.limit(2).agg(F.collect_list(F.struct("*")).alias("__structure_policies"))
    policies = F.col("__structure_policies")
    # Put the guard inside the array expression: an empty explode must not hide the missing-policy failure.
    valid = F.when(F.size(policies) == F.lit(1), policies).otherwise(F.raise_error(message))
    return counted.select(F.explode(valid).alias("__structure_policy"))
