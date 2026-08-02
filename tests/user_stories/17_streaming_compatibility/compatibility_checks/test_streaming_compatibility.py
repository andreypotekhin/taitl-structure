import sys
from typing import cast

from examples.streams.adoption import start_foreach_batch_query
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


@transform(streaming=True)
class StreamingProjection(Transform):
    rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        where(row.id.is_not_null())  # type: ignore[attr-defined]
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingUnknownHook(Transform):
    rows = input(StreamRaw, streaming=True)
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
        required=bool((plan.options or {})["streaming"]),
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
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.UNKNOWN
    assert len(report.findings) == 1
    assert report.findings[0].to_diagnostic().code == "STREAM-W0801"
    assert report.findings[0].to_diagnostic().docs == "docs/Diagnostics.md#stream-w0801"


def test_generated_streaming_code_avoids_lifecycle_and_actions() -> None:
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

    forbidden = ("readStream", "writeStream", "foreachBatch", ".foreach(", "collect(", "count(", "toPandas", ".rdd")
    assert not any(value in generated for value in forbidden)


def test_foreach_batch_recipe_is_caller_owned_and_executable_without_spark() -> None:
    """Caller code can attach foreachBatch after Structure returns a transformed DataFrame."""

    def callback(frame, batch_id):
        return None

    writer = FakeStreamingWriter()
    query = start_foreach_batch_query(
        FakeStreamingFrame(writer),
        callback,
        checkpoint="/tmp/structure-checkpoint",
        output_mode="append",
        trigger={"availableNow": True},
    )

    assert query == "query"
    assert writer.operations == (
        "foreachBatch:callback",
        "outputMode:append",
        "option:checkpointLocation=/tmp/structure-checkpoint",
        "trigger:availableNow=True",
        "start",
    )


class FakeStreamingFrame:
    def __init__(self, writer: "FakeStreamingWriter") -> None:
        self.writeStream = writer


class FakeStreamingWriter:
    def __init__(self) -> None:
        self.operations: tuple[str, ...] = ()

    def foreachBatch(self, callback):
        self.operations = (*self.operations, f"foreachBatch:{callback.__name__}")
        return self

    def outputMode(self, mode: str):
        self.operations = (*self.operations, f"outputMode:{mode}")
        return self

    def option(self, key: str, value: str):
        self.operations = (*self.operations, f"option:{key}={value}")
        return self

    def trigger(self, **options: object):
        rendered = ",".join(f"{key}={value}" for key, value in sorted(options.items()))
        self.operations = (*self.operations, f"trigger:{rendered}")
        return self

    def start(self):
        self.operations = (*self.operations, "start")
        return "query"
