from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    _plugin,
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)
from integration.pyspark.support.rows import rows

from structure import Schema, StructureConfig, StructureSession, Transform, input, output, transform
from structure.plugin.pyspark import PySpark, array, explode_struct, integer, string, struct, union_all, union_by_name

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v8.test_stateless_streaming_gaps"
PACKAGE = "integration_v8_stateless_streaming_generated"
LIFECYCLE_TOKENS = (
    "readStream",
    "writeStream",
    "checkpoint",
    ".trigger(",
    ".outputMode(",
    ".start(",
    "awaitTermination",
    ".collect(",
    ".count(",
    ".toPandas(",
    ".rdd",
    "foreachBatch",
)


class Term(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class Document(Schema):
    doc_id = string(nullable=False)
    terms = array(struct(Term), contains_null=False, nullable=False)


class GeneratedTerm(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class DocumentTerm(Schema):
    doc_id = string(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class StreamRaw(Schema):
    id = string(nullable=False)


class StreamClean(Schema):
    id = string(nullable=False)


@transform(streaming=True)
class StreamingExplodeTerms(Transform):
    documents = input(Document, streaming=True)
    terms = output(DocumentTerm)

    def expand(self, document: Document) -> DocumentTerm:
        term = explode_struct(document.terms, as_=GeneratedTerm, scope="term")
        return DocumentTerm(doc_id=document.doc_id, token=term.token, weight=term.weight)


@transform(streaming=True)
class StreamingUnionAllRows(Transform):
    rows = input(StreamRaw, streaming=True)
    more_rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def merge(self, row: StreamRaw, more: StreamRaw) -> StreamClean:
        merged = union_all(more)
        return StreamClean(id=merged.id)


@transform(streaming=True)
class StreamingUnionByNameRows(Transform):
    rows = input(StreamRaw, streaming=True)
    more_rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def merge(self, row: StreamRaw, more: StreamRaw) -> StreamClean:
        merged = union_by_name(more)
        return StreamClean(id=merged.id)


for _source_type in (
    Term,
    Document,
    GeneratedTerm,
    DocumentTerm,
    StreamRaw,
    StreamClean,
    StreamingExplodeTerms,
    StreamingUnionAllRows,
    StreamingUnionByNameRows,
):
    _source_type.__module__ = SOURCE_MODULE


def test_v8_struct_generator_restarts_online_and_generated(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Structured Streaming restart evidence is classic PySpark only")

    files = _render_generator_project()
    _assert_no_lifecycle_calls(files)

    stream_root = _stream_root("generator")
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.test_stateless_streaming_gaps")
            expected_first: list[dict[str, object]] = [
                {"doc_id": "d-1", "token": "spark", "weight": 2},
                {"doc_id": "d-1", "token": "stream", "weight": 3},
            ]
            expected_second: list[dict[str, object]] = [
                {"doc_id": "d-2", "token": "typed", "weight": 5},
            ]

            for mode in ("online", "generated"):
                source = stream_root / mode / "documents"
                _write_parquet(spark, source, [_document("d-1", [("spark", 2), ("stream", 3)])], schemas.DOCUMENT_SCHEMA)
                first = _run_single_stream_once(
                    spark,
                    source,
                    stream_root / mode / "checkpoint",
                    stream_root / mode / "output",
                    _explode_frame(spark, mode),
                    schemas.DOCUMENT_SCHEMA,
                    source_format="parquet",
                    order_by=("doc_id", "token"),
                )
                _write_parquet(spark, source, [_document("d-2", [("typed", 5)])], schemas.DOCUMENT_SCHEMA)
                second = _run_single_stream_once(
                    spark,
                    source,
                    stream_root / mode / "checkpoint",
                    stream_root / mode / "output",
                    _explode_frame(spark, mode),
                    schemas.DOCUMENT_SCHEMA,
                    source_format="parquet",
                    order_by=("doc_id", "token"),
                )

                assert first == expected_first
                assert second == [*expected_first, *expected_second]
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)


@pytest.mark.parametrize("transform_type", [StreamingUnionAllRows, StreamingUnionByNameRows])
def test_v8_stream_stream_union_restarts_online_and_generated(spark, tmp_path, transform_type) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Structured Streaming restart evidence is classic PySpark only")

    files = render_generated_projects(
        ((transform_type, f"{SOURCE_MODULE}.{transform_type.__name__}"),),
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StreamRaw, StreamClean]},
    )
    _assert_no_lifecycle_calls(files)

    stream_root = _stream_root(transform_type.__name__)
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.test_stateless_streaming_gaps")
            expected_first = [{"id": "l-1"}, {"id": "r-1"}]
            expected_all = [{"id": "l-1"}, {"id": "l-2"}, {"id": "r-1"}, {"id": "r-2"}]

            for mode in ("online", "generated"):
                left = stream_root / mode / "left"
                right = stream_root / mode / "right"
                _write_json(left / "batch-1.json", [{"id": "l-1"}])
                _write_json(right / "batch-1.json", [{"id": "r-1"}])
                first = _run_two_streams_once(
                    spark,
                    left,
                    right,
                    stream_root / mode / "checkpoint",
                    stream_root / mode / "output",
                    _union_frame(spark, transform_type, mode),
                    schemas.STREAM_RAW_SCHEMA,
                )
                _write_json(left / "batch-2.json", [{"id": "l-2"}])
                _write_json(right / "batch-2.json", [{"id": "r-2"}])
                second = _run_two_streams_once(
                    spark,
                    left,
                    right,
                    stream_root / mode / "checkpoint",
                    stream_root / mode / "output",
                    _union_frame(spark, transform_type, mode),
                    schemas.STREAM_RAW_SCHEMA,
                )

                assert first == expected_first
                assert second == expected_all
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)


def _run_single_stream_once(
    spark,
    source: Path,
    checkpoint: Path,
    sink: Path,
    build_frame: Callable[[object], Any],
    schema,
    *,
    source_format: str = "json",
    order_by: tuple[str, ...],
) -> list[dict[str, object]]:
    stream = (
        spark.readStream.schema(schema).parquet(str(source))
        if source_format == "parquet"
        else spark.readStream.schema(schema).json(str(source))
    )
    return _run_once(spark, checkpoint, sink, build_frame(stream), order_by=order_by)


def _run_two_streams_once(
    spark,
    left: Path,
    right: Path,
    checkpoint: Path,
    sink: Path,
    build_frame: Callable[[object, object], Any],
    schema,
) -> list[dict[str, object]]:
    left_stream = spark.readStream.schema(schema).json(str(left))
    right_stream = spark.readStream.schema(schema).json(str(right))
    return _run_once(spark, checkpoint, sink, build_frame(left_stream, right_stream), order_by=("id",))


def _run_once(spark, checkpoint: Path, sink: Path, frame, *, order_by: tuple[str, ...]) -> list[dict[str, object]]:
    query = (
        frame.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint))
        .start(str(sink))
    )
    try:
        query.processAllAvailable()
    finally:
        query.stop()
    return rows(spark.read.parquet(str(sink)), *order_by)


def _explode_frame(spark, execution_mode: str) -> Callable[[object], Any]:
    def build(stream: object) -> Any:
        return (
            StreamingExplodeTerms(documents=stream)
            .run(_streaming_session(spark, execution_mode=execution_mode, validate_inputs=False))
            .terms
        )

    return build


def _union_frame(spark, transform_type: type[Transform], execution_mode: str) -> Callable[[object, object], Any]:
    def build(left: object, right: object) -> Any:
        return (
            transform_type(rows=left, more_rows=right)
            .run(session(spark, execution_mode=execution_mode, generated_package=PACKAGE))
            .clean
        )

    return build


def _streaming_session(spark, *, execution_mode: str, validate_inputs: bool = True) -> StructureSession:
    return StructureSession(
        spark=spark,
        config=StructureConfig.create(
            execution_mode=execution_mode,
            generated_package=PACKAGE,
            validate_inputs=validate_inputs,
            plugin=_plugin(),
        ),
    )


def _render_generator_project() -> dict[str, str]:
    source_transform = f"{SOURCE_MODULE}.StreamingExplodeTerms"
    artifact = StreamingExplodeTerms.compile(
        generated_package=PACKAGE,
        validate_inputs=False,
        plugin=_plugin(),
    )
    files = PySpark.render.project()(
        artifact.pyspark_plan,
        source_transform=source_transform,
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Term, Document, GeneratedTerm, DocumentTerm]},
        semantic_fingerprint=artifact.semantic_fingerprint,
    )
    for path, source in tuple(files.items()):
        if "/pyspark/transforms/" in path:
            files[path] = source.replace(
                '        assert_schema(documents, DOCUMENT_SCHEMA, name="Document", mode="strict")\n',
                "",
            )
    return files


def _assert_no_lifecycle_calls(files: dict[str, str]) -> None:
    for path, source in files.items():
        if "/pyspark/transforms/" not in path:
            continue
        for token in LIFECYCLE_TOKENS:
            assert token not in source, f"{path} contains streaming lifecycle token {token!r}"


def _write_json(path: Path, values: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(value) for value in values), encoding="utf-8")


def _write_parquet(spark, path: Path, values: list[dict[str, object]], schema) -> None:
    path.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame(values, schema).write.mode("append").parquet(str(path))


def _document(doc_id: str, terms: list[tuple[str, int]]) -> dict[str, object]:
    return {"doc_id": doc_id, "terms": [{"token": token, "weight": weight} for token, weight in terms]}


def _stream_root(name: str) -> Path:
    return Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v8-{name}-{uuid4().hex}"
