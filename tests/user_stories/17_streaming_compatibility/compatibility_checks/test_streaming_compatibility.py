import sys
from typing import cast

from structure import *
from structure.core.compiler.api import Compiler
from structure.core.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class StreamRaw(Schema):
    id = string(nullable=False)


class StreamClean(Schema):
    id = string(nullable=False)


@transform(streaming_compatible=True)
class StreamingProjection(Transform):
    rows = input(StreamRaw)
    clean = output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        where(row.id.is_not_null())  # type: ignore[attr-defined]
        return StreamClean(id=row.id)


@transform(streaming_compatible=True)
class StreamingUnknownHook(Transform):
    rows = input(StreamRaw)
    clean = output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        return StreamClean(id=row.id)

    @raw
    def arbitrary_hook(self, *, rows, spark, ctx):
        return rows


def test_streaming_projection_filter_and_validation_are_compatible_without_spark() -> None:
    """I can enable streaming compatibility checks."""

    before = {name for name in sys.modules if name.startswith("pyspark")}

    compilation = Compiler.frontend.compile()(StreamingProjection, materialize_schemas=False)
    plan = cast(TransformPlan, compilation.analysis)
    report = Compiler.compileability.streaming()(
        compilation.lowered,
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.required
    assert report.findings == ()
    assert {name for name in sys.modules if name.startswith("pyspark")} == before


def test_streaming_unknown_hook_reports_a_registered_warning() -> None:
    """Streaming-unknown hooks report registered warnings."""

    compilation = Compiler.frontend.compile()(StreamingUnknownHook, materialize_schemas=False)
    plan = cast(TransformPlan, compilation.analysis)
    report = Compiler.compileability.streaming()(
        compilation.lowered,
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.UNKNOWN
    assert len(report.findings) == 1
    assert report.findings[0].to_diagnostic().code == "STREAM-W0801"
    assert report.findings[0].to_diagnostic().docs == "docs/Diagnostics.md#stream-w0801"


def test_generated_streaming_compatible_code_avoids_lifecycle_and_actions() -> None:
    """I can keep streaming orchestration outside Structure in v1 and v2."""

    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(StreamingProjection, materialize_schemas=False).lowered,
    )
    files = PySpark.render.project()(
        plan,
        source_transform="tests.fixtures.streaming.transforms.StreamingProjection",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamClean]},
    )
    generated = "\n".join(files.values())

    forbidden = ("readStream", "writeStream", "collect(", "count(", "toPandas", ".rdd")
    assert not any(value in generated for value in forbidden)
