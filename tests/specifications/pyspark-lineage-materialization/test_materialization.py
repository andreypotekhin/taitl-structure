from dataclasses import replace
from types import SimpleNamespace
from typing import cast

import pytest

from structure import Schema, Transform, input, lane, output, step, transform
from structure.core.compiler.api import Compiler
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model import TransformPlan
from structure.plugin.pyspark import cache, checkpoint, local_checkpoint, persist, string, union_all, unpersist
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.commands.BuildPySparkLineageDiagnostics import BuildPySparkLineageDiagnostics
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.operations.MaterializationPlan import CheckpointPlan
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.plugin.pyspark.render.commands.RenderPySparkExplainReport import RenderPySparkExplainReport
from structure.plugin.pyspark.render.logic.steps.RenderPySparkStep import render_pyspark_step
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class MaterializationInput(Schema):
    id = string(nullable=False)


class MaterializationOutput(Schema):
    id = string(nullable=False)


@transform(validate_intermediate=False)
class FusedProjectionUnion(Transform):
    rows = input(MaterializationInput)
    projected = lane(MaterializationInput)
    merged = output(MaterializationInput)

    @step(input=rows, output=projected)
    def project(self, row: MaterializationInput) -> MaterializationInput:
        return MaterializationInput.project(row)

    @step(input=[rows, projected], output=merged)
    def merge(self, row: MaterializationInput, projection: MaterializationInput) -> MaterializationInput:
        return MaterializationInput.project(union_all(projection))


def _compile(transform_type: type[Transform]):
    return Compiler.frontend.compile()(transform_type, materialize_schemas=False)


@transform
class Materialize(Transform):
    rows = input(MaterializationInput)
    published = output(MaterializationOutput)

    def publish(self, row: MaterializationInput) -> MaterializationOutput:
        persist()
        cache()
        unpersist(blocking=True)
        checkpoint(eager=False)
        local_checkpoint()
        return MaterializationOutput(id=row.id)


def test_materialization_helpers_preserve_order_and_arguments() -> None:
    compilation = _compile(Materialize)
    body = cast(PySparkStepBody, compilation.analysis.steps[0].plugin_body)
    assert [operation.kind for operation in body.operations] == [
        "persist",
        "persist",
        "unpersist",
        "checkpoint",
        "local_checkpoint",
    ]
    assert body.operations[0].persist is not None
    assert body.operations[1].persist is not None
    assert body.operations[2].unpersist is not None and body.operations[2].unpersist.blocking is True
    assert body.operations[3].checkpoint is not None and body.operations[3].checkpoint.eager is False
    assert body.operations[4].local_checkpoint is not None and body.operations[4].local_checkpoint.eager is True


def test_materialization_recipe_and_generated_source_match() -> None:
    lowered = cast(PySparkExecutionPlan, _compile(Materialize).lowered)
    operations = lowered.steps[0].operations
    assert [operation.kind for operation in operations] == [
        "persist",
        "persist",
        "unpersist",
        "checkpoint",
        "local_checkpoint",
    ]
    source = render_pyspark_step(lowered.steps[0], current="df")
    assert "rows = rows.persist()" in source
    assert "rows = rows.unpersist(blocking=True)" in source
    assert "rows = rows.checkpoint(eager=False)" in source
    assert "rows = rows.localCheckpoint(eager=True)" in source


def test_lineage_warning_is_deduplicated_and_checkpoint_resets_risk() -> None:
    join = SimpleNamespace(source="rows", input_name="rows", method=SimpleNamespace(value="rowset_join"))
    first = OperationPlan.join_operation(join)
    second = OperationPlan.join_operation(join)
    checkpoint_operation = OperationPlan.checkpoint_operation(CheckpointPlan())
    step = SimpleNamespace(
        name="publish",
        source="rows",
        source_scope="rows",
        origin=SimpleNamespace(owner=Materialize),
        plugin_body=PySparkStepBody(value=None, operations=(first, second)),
    )
    plan = cast(TransformPlan, SimpleNamespace(name="Materialize", steps=(step,)))
    diagnostics = BuildPySparkLineageDiagnostics()(plan, enabled=True)
    assert [diagnostic.code for diagnostic in diagnostics] == ["PYSPARK-W2701"]
    assert "checkpoint()" in diagnostics[0].use_text()

    step.plugin_body = PySparkStepBody(value=None, operations=(first, checkpoint_operation, second))
    assert BuildPySparkLineageDiagnostics()(plan, enabled=True) == ()


def test_lineage_warning_separates_diminishing_from_residual_risk() -> None:
    join = SimpleNamespace(source="rows", input_name="rows", method=SimpleNamespace(value="rowset_join"))
    operation = OperationPlan.join_operation(join)
    step = SimpleNamespace(
        name="publish",
        source="rows",
        source_scope="rows",
        origin=SimpleNamespace(owner=Materialize),
        plugin_body=PySparkStepBody(value=None, operations=(operation, operation)),
    )
    plan = cast(TransformPlan, SimpleNamespace(name="Materialize", steps=(step,)))
    optimized = cast(
        PySparkExecutionPlan,
        SimpleNamespace(
            optimizations=(
                SimpleNamespace(
                    kind="projection-union-fusion",
                    detail="projection-union fusion: project + merge",
                ),
            ),
        ),
    )

    diagnostic = BuildPySparkLineageDiagnostics()(plan, enabled=True, execution_plan=optimized)[0]

    assert diagnostic.context["lineage_action"] == "diminished-residual-risk"
    assert "still grows exponentially" in diagnostic.problem_text()
    assert "Diminish:" in diagnostic.use_text()
    assert "Bound it" in diagnostic.use_text()
    assert "stable base relation" in diagnostic.use_text()


@pytest.mark.parametrize("intervening_kind", ["cache", "persist", "relation_alias", "temporary_view", "python_assignment"])
def test_non_checkpoint_reuse_does_not_claim_lineage_is_bounded(intervening_kind: str) -> None:
    join = SimpleNamespace(source="rows", input_name="rows", method=SimpleNamespace(value="rowset_join"))
    first = OperationPlan.join_operation(join)
    second = OperationPlan.join_operation(join)
    intervening = () if intervening_kind == "python_assignment" else (
        SimpleNamespace(kind=intervening_kind, join=None, relation_set=None, source_span=None),
    )
    step = SimpleNamespace(
        name="publish",
        source="rows",
        source_scope="rows",
        origin=SimpleNamespace(owner=Materialize),
        plugin_body=PySparkStepBody(value=None, operations=(first, *intervening, second)),
    )
    plan = cast(TransformPlan, SimpleNamespace(name="Materialize", steps=(step,)))

    diagnostics = BuildPySparkLineageDiagnostics()(plan, enabled=True)

    assert [diagnostic.code for diagnostic in diagnostics] == ["PYSPARK-W2701"]
    assert "bounded" not in diagnostics[0].problem_text().lower()
    assert "bounded" not in diagnostics[0].use_text().lower()


def test_lineage_warning_can_be_disabled_at_project_and_transform_scope() -> None:
    assert StructureConfig.create().warn_on_lineage_growth is True
    assert StructureConfig.create(warn_on_lineage_growth=False).warn_on_lineage_growth is False

    @transform(warn_on_lineage_growth=False)
    class Disabled(Materialize):
        pass

    assert Disabled.effective_transform_options()["warn_on_lineage_growth"] is False


def test_materialization_capabilities_are_ordinary_only() -> None:
    ordinary = PySparkCapabilities(target_variant="ordinary")
    connect = PySparkCapabilities(target_variant="spark-connect")
    for name in ("persist", "unpersist", "checkpoint", "local_checkpoint"):
        requirement = CapabilityRequirement(group="optimization", name=name)
        assert ordinary.supports(requirement).supported
        assert not connect.supports(requirement).supported


def test_projection_union_optimizer_fuses_private_branch() -> None:
    lowered = cast(PySparkExecutionPlan, _compile(FusedProjectionUnion).lowered)

    assert [step.name for step in lowered.steps] == ["merge"]
    assert lowered.steps[0].operations[0].kind == "explode_struct"
    assert lowered.optimizations[0].detail == "projection-union fusion: project + merge"


def test_explain_lineage_state_reports_diminish_residual_risk_and_boundary() -> None:
    compilation = _compile(FusedProjectionUnion)
    plan = replace(
        compilation.analysis,
        diagnostics=(
            Diagnostic(
                entry=diagnostic_registry.get("PYSPARK-W2701"),
                context={
                    "step": "merge",
                    "lineage_action": "diminished-residual-risk",
                },
            ),
        ),
    )
    recipe = cast(PySparkExecutionPlan, compilation.lowered)

    state = RenderPySparkExplainReport()._lineage_states(plan, recipe)

    assert "merge: diminish: projection-union fusion: project + merge" in state
    assert "merge: residual risk: exponential repeated-lineage reuse remains" in state
    assert "merge: nearest true boundary: none (add checkpoint() or local_checkpoint())" in state
