from typing import cast

import pytest
from helpers import fake_pyspark_schema as FakeTypes

from examples.search.transforms.searching.search_docs.fuse import FuseDocuments
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments
from structure import Schema, StructureSession, Transform, input, output, parameter, step
from structure.core.compiler.api import Compiler
from structure.core.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform
from structure.core.compiler.artifacts.model import CompilerOptions
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.plugin.pyspark import checkpoint, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.execution.logic.running.RunOnlinePySparkTransform import RunOnlinePySparkTransform
from structure.plugin.pyspark.render.logic.steps.RenderPySparkStep import render_pyspark_step


class Candidate(Schema):
    id = string(nullable=False)


class ConditionalBoundary(Transform):
    materialize = parameter(False)
    rows = input(Candidate)
    result = output(Candidate)

    @step(input=rows, output=result)
    def boundary(self, row: Candidate) -> Candidate:
        if self.materialize:
            checkpoint(eager=True)
        return row


class NestedBoundary(Transform):
    enabled = parameter(False)
    rows = input(Candidate)
    bounded = ConditionalBoundary(rows=rows, materialize=enabled)
    result = output(Candidate, bounded.result)


class OuterBoundary(Transform):
    enabled = parameter(False)
    rows = input(Candidate)
    nested = NestedBoundary(rows=rows, enabled=enabled)
    result = output(Candidate, nested.result)


class InvertedBoundary(Transform):
    streaming = parameter(False)
    rows = input(Candidate)
    bounded = ConditionalBoundary(rows=rows, materialize=~streaming)
    result = output(Candidate, bounded.result)


def _checkpoints(subject) -> list[str]:
    plan = cast(PySparkExecutionPlan, Compiler.frontend.compile()(subject, materialize_schemas=False).lowered)
    return [step.name for step in plan.steps for operation in step.operations if operation.kind == "checkpoint"]


@pytest.mark.parametrize("transform", [ConditionalBoundary, NestedBoundary, OuterBoundary])
def test_constructor_parameters_reach_nested_symbolic_steps(transform) -> None:
    name = "materialize" if transform is ConditionalBoundary else "enabled"
    assert _checkpoints(transform) == []
    assert len(_checkpoints(transform(**{name: True}))) == 1
    assert _checkpoints(transform(**{name: False})) == []


def test_parameter_specializations_have_distinct_cache_keys_and_fingerprints() -> None:
    builder = BuildCompiledTransform()
    options = CompilerOptions.resolve()
    default = builder(OuterBoundary, options=options, materialize_schemas=False)
    explicit_default = builder(OuterBoundary(enabled=False), options=options, materialize_schemas=False)
    enabled = builder(OuterBoundary(enabled=True), options=options, materialize_schemas=False)
    assert default.key == explicit_default.key
    assert default.semantic_fingerprint == explicit_default.semantic_fingerprint
    assert enabled.key != default.key
    assert enabled.semantic_fingerprint != default.semantic_fingerprint
    plan = cast(PySparkExecutionPlan, enabled.pyspark_plan)
    assert "checkpoint(eager=True)" in render_pyspark_step(plan.steps[0], current="df")


def test_session_compiles_bound_parameters_without_including_runtime_frames() -> None:
    session = StructureSession(schema_types=FakeTypes)
    first = session.compile(OuterBoundary(rows=object(), enabled=True))
    second = session.compile(OuterBoundary(rows=object(), enabled=True))
    lazy = session.compile(OuterBoundary(enabled=False))
    assert first is second
    assert first.key != lazy.key
    assert session.cache_status().entries == 2


def test_parameter_cache_does_not_equate_false_and_zero() -> None:
    session = StructureSession(schema_types=FakeTypes)
    session.compile(InvertedBoundary(streaming=False))
    with pytest.raises(TypeError, match="must be Boolean"):
        session.compile(InvertedBoundary(streaming=0))


def test_search_keeps_materialization_inside_fuse_documents() -> None:
    assert _checkpoints(FuseDocuments) == []
    boundaries = ["validate_lexical_candidates", "validate_vector_candidates", "materialize_candidates"]
    assert _checkpoints(FuseDocuments(materialize=True)) == boundaries
    assert _checkpoints(SearchDocuments) == [f"fused.{name}" for name in boundaries]
    assert _checkpoints(SearchDocuments(streaming=True)) == []


def test_fusion_rejects_non_boolean_materialization_parameters() -> None:
    with pytest.raises(StructureCompileError, match="parameter must be Boolean"):
        _checkpoints(FuseDocuments(materialize="yes"))


def test_checkpoint_rejects_actual_streaming_frames_before_starting_an_action() -> None:
    from unittest.mock import Mock

    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(ConditionalBoundary(materialize=True), materialize_schemas=False).lowered,
    )
    frame = Mock(isStreaming=True)
    with pytest.raises(ValueError, match="requires a batch DataFrame"):
        RunOnlinePySparkTransform()._operations(
            plan.steps[0], frame, frames={}, functions=None, window=None, types=None, session=None
        )
    frame.checkpoint.assert_not_called()
    source = render_pyspark_step(plan.steps[0], current="df")
    assert source.index(".isStreaming:") < source.index(".checkpoint(eager=True)")


def test_boolean_negation_is_resolved_at_compilation_not_class_declaration() -> None:
    assert len(_checkpoints(InvertedBoundary)) == 1
    assert _checkpoints(InvertedBoundary(streaming=True)) == []
    assert len(_checkpoints(InvertedBoundary(streaming=False))) == 1
    with pytest.raises(TypeError, match="must be Boolean"):
        _checkpoints(InvertedBoundary(streaming="yes"))


def test_boolean_declarations_reject_eager_truth_testing() -> None:
    flag = parameter(False)
    assert ~~flag is flag
    with pytest.raises(TypeError, match="deferred Boolean negation"):
        bool(flag)
    with pytest.raises(TypeError, match="not a value"):
        bool(~flag)
    with pytest.raises(TypeError, match="Only Boolean"):
        ~parameter(1)
