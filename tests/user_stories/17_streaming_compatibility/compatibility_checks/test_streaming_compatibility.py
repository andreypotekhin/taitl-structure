import sys

from structure import String, Structure, Transform, after, field, input, output, transform, where
from structure.app.compiler.api import Compiler
from structure.app.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class StreamRaw(Structure):
    id = field(String(), nullable=False)


class StreamClean(Structure):
    id = field(String(), nullable=False)


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

    @after(normalize, lane=rows)
    def arbitrary_hook(self, *, rows, spark, ctx):
        return rows


def test_streaming_projection_filter_and_validation_are_compatible_without_spark() -> None:
    """I can enable streaming compatibility checks."""

    before = {name for name in sys.modules if name.startswith("pyspark")}

    plan = compile_transform(StreamingProjection)
    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.required
    assert report.findings == ()
    assert {name for name in sys.modules if name.startswith("pyspark")} == before


def test_streaming_unknown_hook_reports_a_registered_warning() -> None:
    """Streaming-unknown hooks report registered warnings."""

    plan = compile_transform(StreamingUnknownHook)
    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.UNKNOWN
    assert len(report.findings) == 1
    assert report.findings[0].to_diagnostic().code == "STREAM-W0801"
    assert report.findings[0].to_diagnostic().docs == "docs/Diagnostics.md#stream-w0801"


def test_generated_streaming_compatible_code_avoids_lifecycle_and_actions() -> None:
    """I can keep streaming orchestration outside Structure in v1 and v2."""

    plan = PySpark.plan.lower()(compile_transform(StreamingProjection))
    files = PySpark.render.project()(
        plan,
        source_transform="tests.fixtures.streaming.transforms.StreamingProjection",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamClean]},
    )
    generated = "\n".join(files.values())

    forbidden = ("readStream", "writeStream", "collect(", "count(", "toPandas", ".rdd")
    assert not any(value in generated for value in forbidden)
