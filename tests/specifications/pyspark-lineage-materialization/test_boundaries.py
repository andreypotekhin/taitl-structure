from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from structure import Schema, Transform, input, lane, output, step, transform
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import StageOutputPlan
from structure.plugin.pyspark import checkpoint, string
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.commands.LowerPySparkPlan import LowerPySparkPlan
from structure.plugin.pyspark.compiler.model.PySparkMaterializationRecipe import PySparkCheckpointRecipe
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.execution.logic.PlanBoundary import apply_plan_boundary, close_plan_boundaries
from structure.plugin.pyspark.execution.logic.running.RunOnlinePySparkTransform import RunOnlinePySparkTransform
from structure.plugin.pyspark.render.logic.steps.RenderPySparkStep import render_pyspark_step


class BoundaryRow(Schema):
    id = string(nullable=False)


class Fork(Transform):
    rows = input(BoundaryRow, streaming=True)
    shared = lane(BoundaryRow)
    left = output(BoundaryRow)
    right = output(BoundaryRow)

    @step(input=rows, output=shared)
    def prepare(self, row: BoundaryRow) -> BoundaryRow:
        return row

    @step(input=shared, output=left)
    def first(self, row: BoundaryRow) -> BoundaryRow:
        return row

    @step(input=shared, output=right)
    def second(self, row: BoundaryRow) -> BoundaryRow:
        return row


@transform(streaming=True)
class StreamingFork(Fork):
    pass


class CheckpointRows(Transform):
    rows = input(BoundaryRow, streaming=True)
    result = output(BoundaryRow)

    def publish(self, row: BoundaryRow) -> BoundaryRow:
        checkpoint(eager=False)
        return row


def compile_fork(transform_type=Fork, *, variant="ordinary", **options):
    return Compiler.frontend.compile()(
        transform_type,
        materialize_schemas=False,
        plugin={"pyspark": {"variant": variant, **options}},
    )


def boundaries(plan):
    return [any(validation.boundary for validation in step.validations) for step in plan.steps]


@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
@pytest.mark.parametrize(
    "policy, expected",
    [
        (None, [True, False, False]),
        ("off", [False] * 3),
        ("auto", [True, False, False]),
        ("strict", [True, True, False]),
    ],
)
def test_policy(variant, policy, expected):
    options = {} if policy is None else {"plan_boundaries": policy}
    assert boundaries(compile_fork(variant=variant, **options).lowered) == expected


def test_explicit_streaming_contract_defaults_off_but_allows_override():
    assert boundaries(compile_fork(StreamingFork).lowered) == [False] * 3
    assert boundaries(compile_fork(StreamingFork, plan_boundaries="auto").lowered) == [True, False, False]


@pytest.mark.parametrize(
    "options, message",
    [
        ({"plan_boundaries": "sometimes"}, "plan_boundaries must be"),
        ({"plan_boundaries": ""}, "plan_boundaries must be"),
        ({"plan_boundaries": None}, "plan_boundaries must be"),
        ({"plan_boundaries": True}, "plan_boundaries must be"),
        ({"connect_plan_boundaries": "off"}, "use plan_boundaries instead"),
        ({"connect_plan_boundaries": "off", "plan_boundaries": "auto"}, "use plan_boundaries instead"),
    ],
)
def test_invalid_policy(options, message):
    with pytest.raises(ValueError, match=message):
        compile_fork(**options)


def test_consumers_include_final_and_exposed_outputs():
    plan = compile_fork().analysis
    frame = plan.steps[0].results[0].frame
    published = replace(plan.outputs[0], source=frame)
    plan = replace(plan, steps=plan.steps[:1], outputs=(published, replace(published, name="copy")))
    assert frame in LowerPySparkPlan._boundary_frames(plan)
    lowered = LowerPySparkPlan()(plan, capabilities=PySparkCapabilities(), boundary_policy="auto")
    assert boundaries(lowered) == [True]
    plan = replace(
        plan, outputs=(published,), stage_outputs=(StageOutputPlan(path=("stage", "rows"), output=published),)
    )
    assert frame in LowerPySparkPlan._boundary_frames(plan)
    assert frame not in LowerPySparkPlan._boundary_frames(replace(plan, allow_stage_outputs=False))


def test_one_step_and_one_output_are_shared_but_repeated_step_inputs_are_not():
    plan = compile_fork().analysis
    frame = plan.steps[0].results[0].frame
    consumer = plan.steps[1]
    plan = replace(plan, steps=(plan.steps[0], replace(consumer, inputs=consumer.inputs * 2)), outputs=())
    assert frame not in LowerPySparkPlan._boundary_frames(plan)
    published = replace(compile_fork().analysis.outputs[0], source=frame)
    assert frame in LowerPySparkPlan._boundary_frames(replace(plan, outputs=(published,)))


def test_linear_plan_has_no_periodic_boundary():
    plan = compile_fork().analysis
    template = plan.steps[0]
    steps = []
    source = template.source
    for ordinal in range(12):
        frame = f"linear_{ordinal}"
        steps.append(
            replace(
                template,
                name=frame,
                ordinal=ordinal,
                source=source,
                inputs=tuple(replace(binding, source=source) for binding in template.inputs),
                results=tuple(replace(result, frame=frame) for result in template.results),
            )
        )
        source = frame
    plan = replace(plan, steps=tuple(steps), outputs=(replace(plan.outputs[0], source=source),))
    lowered = LowerPySparkPlan()(plan, capabilities=PySparkCapabilities(), boundary_policy="auto")
    assert not any(boundaries(lowered))


@pytest.mark.parametrize(("streaming", "checkpointed"), [(False, False), (True, False), (False, True)])
def test_online_and_rendered_boundary_guard(streaming, checkpointed):
    recipe = compile_fork().lowered.steps[0]
    recipe = replace(
        recipe,
        projection=(),
        validations=tuple(replace(validation, check=False, project=False) for validation in recipe.validations),
        operations=(
            (PySparkOperationRecipe(kind="checkpoint", checkpoint=PySparkCheckpointRecipe()),) if checkpointed else ()
        ),
    )
    frame = Mock(isStreaming=streaming)
    frame.alias.return_value = frame
    frame.checkpoint.return_value = frame
    spark = Mock()
    frame.sparkSession = spark
    invocation = Fork(rows=frame)
    runner = RunOnlinePySparkTransform()
    online = runner._step(
        recipe,
        current=frame,
        frames={recipe.source: frame},
        inputs={},
        invocation=invocation,
        session=SimpleNamespace(spark=spark),
        functions=Mock(),
        window=Mock(),
        types=Mock(),
    )
    assert next(iter(online.values())) is (frame if streaming else spark.table.return_value)
    assert frame.createOrReplaceTempView.call_count == (0 if streaming else 1)
    if checkpointed:
        calls = [call[0] for call in frame.method_calls]
        assert calls.index("checkpoint") < calls.index("createOrReplaceTempView")
    close_plan_boundaries(spark)

    frame.reset_mock()
    source = render_pyspark_step(recipe, current="frame")
    namespace = {"frame": frame, "self": SimpleNamespace(spark=spark), "apply_plan_boundary": apply_plan_boundary}
    exec("\n".join(line[8:] for line in source.splitlines()), namespace)
    assert frame.createOrReplaceTempView.call_count == (0 if streaming else 1)
    if checkpointed:
        calls = [call[0] for call in frame.method_calls]
        assert calls.index("checkpoint") < calls.index("createOrReplaceTempView")
    close_plan_boundaries(spark)


def test_close_removes_only_owned_views_and_is_idempotent():
    spark, frame = Mock(), Mock()
    apply_plan_boundary(frame, spark)
    name = frame.createOrReplaceTempView.call_args.args[0]
    close_plan_boundaries(spark)
    close_plan_boundaries(spark)
    spark.catalog.dropTempView.assert_called_once_with(name)
    spark.stop.assert_not_called()


@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
def test_checkpoint_staging_is_independent_of_compiler_boundaries(variant):
    plan = compile_fork(CheckpointRows, variant=variant, profile=">=4.0,<4.1", plan_boundaries="off").lowered
    assert not any(boundaries(plan))
    recipe = plan.steps[0].operations[0].checkpoint
    assert recipe.stage_input is (variant == "spark-connect")
    assert recipe.eager is False


@pytest.mark.parametrize("streaming", [False, True])
def test_connect_checkpoint_stages_before_operations_and_preserves_alias(streaming):
    plan = compile_fork(CheckpointRows, variant="spark-connect", profile=">=4.0,<4.1", plan_boundaries="off").lowered
    recipe = replace(plan.steps[0], projection=(), validations=())
    spark = Mock()
    frame = Mock(isStreaming=streaming, sparkSession=spark)
    frame.alias.return_value = frame
    frame.checkpoint.return_value = frame
    spark.table.return_value.alias.return_value = frame
    runner = RunOnlinePySparkTransform()
    source = render_pyspark_step(recipe, current="frame")
    namespace = {"frame": frame, "self": SimpleNamespace(spark=spark), "apply_plan_boundary": apply_plan_boundary}
    calls = [
        lambda: runner._operations(
            recipe, frame, frames={}, functions=Mock(), window=Mock(), types=Mock(), session=None
        ),
        lambda: exec("\n".join(line[8:] for line in source.splitlines()), namespace),
    ]
    for call in calls:
        frame.reset_mock()
        spark.reset_mock()
        try:
            if streaming:
                with pytest.raises(ValueError, match="requires a batch DataFrame"):
                    call()
                frame.createOrReplaceTempView.assert_not_called()
                frame.checkpoint.assert_not_called()
            else:
                call()
                spark.table.return_value.alias.assert_called_once_with(recipe.input_alias)
                frame.checkpoint.assert_called_once_with(eager=False)
                methods = [item[0] for item in frame.method_calls]
                assert methods.index("createOrReplaceTempView") < methods.index("checkpoint")
        finally:
            close_plan_boundaries(spark)
