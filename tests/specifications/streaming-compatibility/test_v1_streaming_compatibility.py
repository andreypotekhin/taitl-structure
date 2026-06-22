import sys

from structure import String, Structure, Transform, after, field, input, transform, where
from structure.app.backend.pyspark.api import lower_pyspark_plan, render_pyspark_project
from structure.app.dsl.api import compile_transform
from structure.app.streaming.api import StreamingSupport, classify_streaming_compatibility


class StreamRaw(Structure):
    id = field(String(), nullable=False)


class StreamClean(Structure):
    id = field(String(), nullable=False)


@transform(streaming_compatible=True)
class StreamingProjection(Transform):
    rows = input(StreamRaw)

    def normalize(self, row: StreamRaw) -> StreamClean:
        where(row.id.is_not_null())  # type: ignore[attr-defined]
        return StreamClean(id=row.id)


@transform(streaming_compatible=True)
class StreamingUnknownHook(Transform):
    rows = input(StreamRaw)

    def normalize(self, row: StreamRaw) -> StreamClean:
        return StreamClean(id=row.id)

    @after(normalize)
    def arbitrary_hook(self, *, df, spark, ctx):
        return df


def test_v1_streaming_projection_filter_and_schema_validation_are_compatible_without_spark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    plan = compile_transform(StreamingProjection)
    report = classify_streaming_compatibility(
        lower_pyspark_plan(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert report.support is StreamingSupport.COMPATIBLE
    assert report.required
    assert report.findings == ()


def test_v1_streaming_unsafe_hook_is_unknown_with_registered_finding() -> None:
    plan = compile_transform(StreamingUnknownHook)

    report = classify_streaming_compatibility(
        lower_pyspark_plan(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.UNKNOWN
    assert len(report.findings) == 1
    finding = report.findings[0]
    assert finding.code == "STREAM-W0801"
    assert finding.step == "normalize"
    assert finding.operation == "after hook arbitrary_hook"
    assert finding.to_diagnostic().docs == "docs/Diagnostics.md#stream-w0801"


def test_v1_streaming_report_is_included_in_explain_output() -> None:
    from structure.app.cli.logic.actions.RenderExplainReport import render_explain_report

    report = render_explain_report(StreamingUnknownHook)

    assert "streaming:" in report
    assert "status: unknown" in report
    assert "required: true" in report
    assert "STREAM-W0801: unknown in normalize (after hook arbitrary_hook)" in report


def test_v1_generated_streaming_compatible_code_avoids_streaming_lifecycle_and_actions() -> None:
    plan = lower_pyspark_plan(compile_transform(StreamingProjection))
    files = render_pyspark_project(
        plan,
        source_transform="tests.fixtures.streaming.transforms.StreamingProjection",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamClean]},
    )

    generated = "\n".join(files.values())

    forbidden = ("readStream", "writeStream", "collect(", "count(", "toPandas", ".rdd")
    assert all(value not in generated for value in forbidden)
