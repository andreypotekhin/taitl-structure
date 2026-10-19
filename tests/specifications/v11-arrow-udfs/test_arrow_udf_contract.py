import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import CapabilityRequirement
from structure.plugin.pyspark import *
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.dsl import types
from structure.plugin.pyspark.dsl.operations_api import current_row, preceding, rows_between, window
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class ArrowInput(Schema):
    key = integer(nullable=False)
    value = long(nullable=False)


class ArrowOutput(Schema):
    result = long(nullable=False)


class GroupOutput(Schema):
    key = long(nullable=False)
    mean = double(nullable=True)
    rows = long(nullable=False)


class ArrowMeanOutput(Schema):
    key = integer(nullable=False)
    mean = double(nullable=True)


def _compile(transform):
    return Compiler.frontend.compile()(
        transform,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
    )


def test_scalar_arrow_helper_captures_named_calls_without_running_callback() -> None:
    calls = []

    class Add(Transform):
        rows = input(ArrowInput)
        output_rows = output(ArrowOutput)

        @special(type="arrow_udf", return_type=types.long(), nullable=False)
        def add(left, right):
            calls.append("worker body ran")
            return left + right

        def apply(self, row: ArrowInput) -> ArrowOutput:
            return ArrowOutput(result=self.add(right=row.value, left=row.value))

    compiled = _compile(Add)
    expression = compiled.analysis.steps[0].plugin_body.projection[0].expression

    assert expression.kind == "arrow_udf"
    assert expression.data["function_type"] == "scalar"
    assert expression.data["deterministic"] is None
    assert expression.data["keyword_names"] == ("right", "left")
    assert calls == []


@pytest.mark.parametrize("function_type", ["scalar", "scalar_iter", "grouped_agg"])
def test_arrow_modes_are_explicit_and_valid(function_type) -> None:
    @special(type="arrow_udf", return_type=types.long(), function_type=function_type, deterministic=False)
    def callback(value):
        return value

    assert callback.function_type == function_type
    assert callback.deterministic is False


def test_arrow_decorator_rejects_invalid_options() -> None:
    with pytest.raises(TypeError, match="requires return_type"):
        special(type="arrow_udf")
    with pytest.raises(TypeError, match="function_type"):
        special(type="arrow_udf", return_type=types.long(), function_type="pandas")
    with pytest.raises(TypeError, match="deterministic"):
        special(type="arrow_udf", return_type=types.long(), deterministic=1)


@pytest.mark.parametrize(
    "requirement",
    [
        CapabilityRequirement(group="expression", name="arrow_udf_scalar"),
        CapabilityRequirement(group="expression", name="arrow_udf_scalar_iter"),
        CapabilityRequirement(group="aggregate", name="arrow_udf_grouped_agg"),
        CapabilityRequirement(group="window", name="arrow_udf_grouped_agg"),
    ],
)
@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
def test_arrow_modes_are_admitted_only_on_exact_41_profiles(requirement, variant) -> None:
    capabilities = PySparkCapabilities(target_profile=">=4.1,<4.2", target_variant=variant)
    assert capabilities.supports(requirement).supported

    for profile in (">=3.5,<4.1", ">=4.0,<4.1", ">=4.2,<4.3"):
        assert not PySparkCapabilities(target_profile=profile, target_variant=variant).supports(requirement).supported


def test_scalar_iter_requires_one_callback_iterator_and_positional_columns() -> None:
    class Iterator(Transform):
        rows = input(ArrowInput)
        output_rows = output(ArrowOutput)

        @special(type="arrow_udf", return_type=types.long(), function_type="scalar_iter", nullable=False)
        def increment(batches):
            return batches

        def apply(self, row: ArrowInput) -> ArrowOutput:
            return ArrowOutput(result=self.increment(row.value))

    assert _compile(Iterator).analysis.steps[0].plugin_body.projection[0].expression.data["function_type"] == "scalar_iter"

    class Invalid(Transform):
        rows = input(ArrowInput)
        output_rows = output(ArrowOutput)

        @special(type="arrow_udf", return_type=types.long(), function_type="scalar_iter")
        def increment(batches):
            return batches

        def apply(self, row: ArrowInput) -> ArrowOutput:
            return ArrowOutput(result=self.increment(value=row.value))

    with pytest.raises(StructureCompileError, match="positional column expression"):
        _compile(Invalid)


def test_scalar_arrow_modes_remain_streaming_compatible() -> None:
    @transform(streaming=True)
    class Streaming(Transform):
        rows = input(ArrowInput, streaming=True)
        output_rows = output(ArrowOutput)

        @special(type="arrow_udf", return_type=types.long())
        def scalar(values):
            return values

        @special(type="arrow_udf", return_type=types.long(), function_type="scalar_iter")
        def iterator(batches):
            return batches

        def apply(self, row: ArrowInput) -> ArrowOutput:
            return ArrowOutput(result=self.scalar(row.value) + self.iterator(row.value))

    compilation = Compiler.frontend.compile()(
        Streaming,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
    )
    report = Compiler.compileability.streaming()(compilation.lowered, required=True)

    assert report.support.value == "compatible"
    assert report.findings == ()


def test_grouped_arrow_streaming_and_mixed_aggregate_are_rejected_early() -> None:
    @transform(streaming=True)
    class StreamingGroup(Transform):
        rows = input(ArrowInput, streaming=True)
        output_rows = output(GroupOutput)

        @special(type="arrow_udf", return_type=types.double(), function_type="grouped_agg")
        def mean(values):
            return values

        def apply(self, row: ArrowInput) -> GroupOutput:
            group_by(row.key)
            return GroupOutput(key=row.key, mean=self.mean(row.value), rows=count())

    with pytest.raises(StructureCompileError, match="mixes a grouped Arrow UDF"):
        _compile(StreamingGroup)


def test_grouped_arrow_analytic_windows_are_classified_batch_only_for_streams() -> None:
    @transform(streaming=True)
    class StreamingWindow(Transform):
        rows = input(ArrowInput, streaming=True)
        output_rows = output(ArrowOutput)

        @special(type="arrow_udf", return_type=types.long(), function_type="grouped_agg")
        def aggregate(values):
            return values

        def apply(self, row: ArrowInput) -> ArrowOutput:
            frame = window(
                partition_by=row.key,
                order_by=row.value,
                frame=rows_between(preceding(1), current_row()),
            )
            return ArrowOutput(result=self.aggregate(row.value, over=frame))

    compilation = _compile(StreamingWindow)
    report = Compiler.compileability.streaming()(compilation.lowered, required=True)

    assert report.support.value == "batch_only"
    assert report.findings[0].code == "STREAM-E0801"


def test_grouped_arrow_aggregate_is_rejected_for_streaming() -> None:
    @transform(streaming=True)
    class StreamingGroup(Transform):
        rows = input(ArrowInput, streaming=True)
        output_rows = output(ArrowMeanOutput)

        @special(type="arrow_udf", return_type=types.double(), function_type="grouped_agg")
        def mean(values):
            return values

        def apply(self, row: ArrowInput) -> ArrowMeanOutput:
            group_by(row.key)
            return ArrowMeanOutput(key=row.key, mean=self.mean(row.value))

    compilation = Compiler.frontend.compile()(
        StreamingGroup,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
    )
    report = Compiler.compileability.streaming()(compilation.lowered, required=True)

    assert report.support.value == "batch_only"
    assert report.findings[0].code == "STREAM-E0801"
