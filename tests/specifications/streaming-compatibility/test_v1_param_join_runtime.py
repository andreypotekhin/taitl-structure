from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.dsl.joins import Join
from structure.plugin.pyspark.execution.logic.running.RunOnlinePySparkTransform import RunOnlinePySparkTransform


class _Frame:
    def __init__(self, *, streaming: bool) -> None:
        self.isStreaming = streaming

    def alias(self, name: str):
        return self

    def crossJoin(self, right):
        return self, right


def _join():
    return SimpleNamespace(
        source="policy",
        input_name="policy",
        right_alias="policy_joined",
        left_alias="event",
        how=Join.CROSS,
        as_of=None,
        strategy=None,
        dedupe=None,
        hint=None,
        assert_singleton_in_batch=True,
    )


def test_param_join_asserts_only_for_batch_step() -> None:
    executor = RunOnlinePySparkTransform()
    assertions: list[str] = []

    def record(frame, scope, *, functions):
        assertions.append(scope)
        return frame

    left = _Frame(streaming=False)
    right = _Frame(streaming=False)
    step = cast(PySparkStepRecipe, SimpleNamespace(source="events", input_sources=("policy",)))

    with patch.object(executor, "_exactly_one", side_effect=record):
        executor._join(
            step,
            left,
            cast(PySparkJoinRecipe, _join()),
            frames={"policy": right},
            functions=None,
            window=None,
            watermarks=(),
            streaming_step=False,
        )

    assert assertions == ["policy"]


def test_param_join_skips_assertion_for_streaming_step() -> None:
    executor = RunOnlinePySparkTransform()
    assertions: list[str] = []

    def record(frame, scope, *, functions):
        assertions.append(scope)
        return frame

    left = _Frame(streaming=True)
    right = _Frame(streaming=False)
    step = cast(PySparkStepRecipe, SimpleNamespace(source="events", input_sources=("policy",)))

    with patch.object(executor, "_exactly_one", side_effect=record):
        executor._join(
            step,
            left,
            cast(PySparkJoinRecipe, _join()),
            frames={"policy": right},
            functions=None,
            window=None,
            watermarks=(),
            streaming_step=True,
        )

    assert assertions == []


def test_runtime_step_mode_reads_all_bound_frames() -> None:
    step = cast(PySparkStepRecipe, SimpleNamespace(source="events", input_sources=("policy",)))
    assert RunOnlinePySparkTransform._is_streaming_step(
        step,
        {"events": _Frame(streaming=False), "policy": _Frame(streaming=True)},
    )
