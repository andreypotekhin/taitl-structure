import sys
from dataclasses import replace
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from helpers import fake_pyspark_schema

from structure import (
    CompiledArtifactPool,
    CompilerOptions,
    Schema,
    StructureConfig,
    Transform,
    input,
    lane,
    output,
    step,
    transform,
)
from structure.core.compiler.api import Compiler
from structure.core.compiler.frontend.commands.PruneUnusedSteps import PruneUnusedSteps
from structure.plugin.api.v1 import CompileRequest, ExplainRequest, OptimizationRequest
from structure.plugin.pyspark import string, where
from structure.plugin.pyspark.api.ExplainAPI import ExplainAPI
from structure.plugin.pyspark.api.OptimizationAPI import OptimizationAPI
from structure.plugin.pyspark.compiler.commands.DescribePySparkOptimization import DescribePySparkOptimization
from structure.plugin.pyspark.compiler.logic.RemovalSafety import RemovalSafety
from structure.plugin.pyspark.compiler.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationAssertionRecipe import PySparkRelationAssertionRecipe
from structure.plugin.pyspark.execution.logic.running.RunOnlinePySparkTransform import RunOnlinePySparkTransform
from structure.plugin.pyspark.render.logic.steps.RenderPySparkStep import render_pyspark_step


class Row(Schema):
    id = string(nullable=False)


class Branches(Transform):
    rows = input(Row)
    private = lane(Row)
    result = output(Row)

    @step(input=rows, output=private)
    def unused(self, row: Row) -> Row:
        return row

    @step(input=rows, output=result)
    def publish(self, row: Row) -> Row:
        return row


class Child(Transform):
    rows = input(Row)
    result = output(Row)
    extra = output(Row)

    @step(input=rows, output=extra)
    def inspectable(self, row: Row) -> Row:
        return row

    @step(input=rows, output=result)
    def publish(self, row: Row) -> Row:
        return row


class Filtered(Branches):
    @step(input=Branches.rows, output=Branches.private)
    def unused(self, row: Row) -> Row:
        where(row.id != "")
        return row


class Parent(Transform):
    rows = input(Row)
    child = Child(rows=rows)
    result = output(Row, child.result)


class Nested(Transform):
    rows = input(Row)
    parent = Parent(rows=rows)
    result = output(Row, parent.result)


class Implicit(Transform):
    rows = input(Row)
    child = Child(rows=rows)
    result = output(Row)


@transform(prune_unused_steps=False)
class Unpruned(Branches):
    pass


@transform(prune_unused_steps=True)
class Pruned(Branches):
    pass


def compile_plan(subject=Branches, **options):
    return Compiler.frontend.compile()(
        subject, materialize_schemas=False,
        **{"validate_intermediate": False, "allow_stage_outputs": False, **options},
    )


def test_private_unused_step_is_removed_but_authored_analysis_is_preserved():
    result = compile_plan()
    assert [step.name for step in result.analysis.steps] == ["unused", "publish"]
    assert [step.name for step in result.lowered.steps] == ["publish"]
    assert result.optimization.removed_steps == ("unused",)
    assert result.lowered.inputs == compile_plan(prune_unused_steps=False).lowered.inputs
    report = ExplainAPI().render(ExplainRequest(Branches, result.lowered, result.analysis))
    assert "2 original, 1 retained" in report
    assert "Removed unused: no returned output or required operation uses it" in report


def test_growth_diagnostics_describe_retained_work_not_removed_authored_steps():
    plugin = {"pyspark": {"PYSPARK_W2704_plan_growth_output_cost": 1}}
    unpruned = compile_plan(Filtered, prune_unused_steps=False, plugin=plugin)
    pruned = compile_plan(Filtered, plugin=plugin)
    assert any(d.code == "PYSPARK-W2704" for d in unpruned.diagnostics)
    assert not any(d.code == "PYSPARK-W2704" for d in pruned.diagnostics)
    assert len(pruned.analysis.steps) == 2


@pytest.mark.parametrize("subject", [Parent, Nested, Implicit])
def test_declared_stage_outputs_remain_available_by_default(subject):
    visible = compile_plan(subject, allow_stage_outputs=True)
    hidden = compile_plan(subject)
    assert len(visible.lowered.steps) == 2
    assert len(hidden.lowered.steps) == 1
    assert visible.optimization.removed_steps == ()
    assert visible.lowered.outputs == hidden.lowered.outputs
    assert "intermediate stage outputs are enabled" in visible.optimization.explain()
    assert hidden.lowered.stage_outputs == visible.lowered.stage_outputs


def test_artifact_cache_separates_rollback_and_preserves_optimization_reports():
    pool = CompiledArtifactPool()
    options = CompilerOptions.from_config(StructureConfig.create(validate_intermediate=False, allow_stage_outputs=False))
    pruned = pool.get_or_compile(Branches, options=options, schema_types=fake_pyspark_schema)
    unpruned = pool.get_or_compile(Branches, options=replace(options, prune_unused_steps=False), schema_types=fake_pyspark_schema)
    assert pruned is pool.get_or_compile(Branches, options=options)
    assert pruned.optimization is not None
    assert isinstance(pruned.pyspark_plan, PySparkExecutionPlan)
    assert isinstance(unpruned.pyspark_plan, PySparkExecutionPlan)
    assert pruned.optimization.removed_steps == ("unused",)
    assert pruned.pyspark_plan.pruning == pruned.optimization
    assert unpruned.optimization is None
    assert len(pruned.pyspark_plan.steps) == 1
    assert len(unpruned.pyspark_plan.steps) == 2
    assert pool.status().entries == 2


def test_validation_is_not_disabled_to_allow_pruning_and_options_have_normal_precedence():
    assert compile_plan(validate_intermediate=True).optimization.removed_steps == ()
    assert compile_plan(prune_unused_steps=False).optimization is None
    assert compile_plan(Unpruned).optimization is None
    assert compile_plan(Pruned, prune_unused_steps=False).optimization.removed_steps == ("unused",)


@pytest.mark.parametrize("policy", ["off", "auto", "strict"])
def test_compiler_boundaries_are_not_independent_roots(policy):
    compiled = compile_plan(plugin={"pyspark": {"plan_boundaries": policy}})
    assert compiled.optimization.removed_steps == ("unused",)


@pytest.mark.parametrize("kind", [
    "require_all", "require_unique", "exactly_one", "checkpoint", "local_checkpoint", "persist", "cache",
    "unpersist", "future_operation",
])
def test_assertions_lifecycle_and_unknown_operations_are_roots(kind):
    plan = compile_plan(prune_unused_steps=False).lowered
    guarded = replace(plan.steps[0], operations=(PySparkOperationRecipe(kind=kind),))
    graph = DescribePySparkOptimization()(replace(plan, steps=(guarded, plan.steps[1])))
    assert not graph.steps[0].removable
    assert PruneUnusedSteps().select(graph).removed_steps == ()


@pytest.mark.parametrize("kind, data", [
    ("python_udf", {}), ("assertion", {}), ("future_expression", {}),
    ("call", {"function": "rand"}), ("transform_expression", {"function": "uuid"}),
    ("field", {"column": object()}),
])
def test_unsafe_expressions_are_retained_including_nested_payloads(kind, data):
    step = compile_plan(prune_unused_steps=False).lowered.steps[0]
    unsafe = PySparkExpressionRecipe(kind, None, False, data)
    outer = PySparkExpressionRecipe("literal", None, False, {"value": unsafe})
    assignment = replace(step.projection[0], expression=outer)
    assert RemovalSafety().reason(replace(step, projection=(assignment,))) is not None


def test_dependencies_follow_ordered_frame_versions_and_multi_result_producers():
    plan = compile_plan(prune_unused_steps=False).lowered
    first, final = plan.steps
    extra_result = replace(first.results[0], frame="extra")
    first = replace(first, results=(*first.results, extra_result))
    second = replace(first, name="replace", source="extra", input_sources=("extra",))
    final = replace(final, source="extra", input_sources=("extra",))
    graph = DescribePySparkOptimization()(replace(plan, steps=(first, second, final)))
    assert graph.steps[1].dependencies == ("unused",)
    assert graph.steps[2].dependencies == ("replace",)
    assert PruneUnusedSteps().select(graph).retained_steps == ("unused", "replace", "publish")


def test_hook_replacements_and_unknown_frames_cannot_lose_dependencies():
    plan = compile_plan(prune_unused_steps=False).lowered
    first, final = plan.steps
    hook = SimpleNamespace(sources=(first.results[0].frame,), outputs=(first.results[0].frame,))
    hooked = replace(first, name="hooked", before_hooks=(hook,))
    final = replace(final, source=first.results[0].frame, input_sources=(first.results[0].frame,))
    graph = DescribePySparkOptimization()(replace(plan, steps=(first, hooked, final)))
    assert graph.steps[1].dependencies == ("unused",)
    assert graph.steps[2].dependencies == ("hooked",)
    with pytest.raises(ValueError, match="unknown frame"):
        DescribePySparkOptimization()(replace(plan, steps=(replace(final, source="missing"),)))


def test_unresolved_composed_operation_sources_retain_all_possible_producers():
    plan = compile_plan(prune_unused_steps=False).lowered
    first, final = plan.steps
    assertion = PySparkRelationAssertionRecipe("require_reference", reference_source="local_unmapped_name")
    final = replace(final, operations=(PySparkOperationRecipe(kind="require_reference", relation_assertion=assertion),))
    graph = DescribePySparkOptimization()(replace(plan, steps=(first, final)))
    assert graph.steps[1].dependencies == ("unused",)
    assert PruneUnusedSteps().select(graph).removed_steps == ()


def test_singleton_checks_and_result_hooks_are_required():
    step = compile_plan(prune_unused_steps=False).lowered.steps[0]
    singleton = SimpleNamespace(assert_singleton_in_batch=True)
    assert "singleton-policy" in (RemovalSafety().reason(replace(step, joins=(singleton,))) or "")
    hook = SimpleNamespace(sources=(), outputs=())
    result = replace(step.results[0], after_hooks=(hook,))
    assert "hook" in (RemovalSafety().reason(replace(step, results=(result,))) or "")


@pytest.mark.parametrize("function, safe", [("count", True), ("first", False), ("future", False)])
def test_aggregate_safety_is_explicit(function, safe):
    step = compile_plan(prune_unused_steps=False).lowered.steps[0]
    assignment = PySparkAggregateAssignment(Row.id, function)
    aggregate = PySparkAggregateRecipe((), (assignment,))
    assert (RemovalSafety().reason(replace(step, aggregate=aggregate)) is None) == safe


def test_optimized_plan_drives_both_rendering_and_runtime_construction(monkeypatch):
    compiled = compile_plan()
    plan = compiled.lowered
    rendered = "\n".join(str(render_pyspark_step(item, current="frame")) for item in plan.steps)
    assert "unused" not in rendered
    sql = ModuleType("pyspark.sql")
    sql.__dict__.update(Window=object(), functions=object(), types=object())
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    executor = RunOnlinePySparkTransform()
    calls = []

    def run_step(step, **kwargs):
        calls.append(step.name)
        return {result.frame: object() for result in step.results}

    monkeypatch.setattr(executor, "_step", run_step)
    monkeypatch.setattr(executor._validator, "validate", Mock())
    monkeypatch.setattr(executor, "_output", lambda output, **kwargs: kwargs["source"])
    result = executor._run(Branches(rows=object()), plan, session=SimpleNamespace(spark=object()))
    assert calls == ["publish"]
    assert list(result) == ["result"]
    with pytest.raises(AttributeError):
        _ = result.private
