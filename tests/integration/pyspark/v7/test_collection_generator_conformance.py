from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_projects,
    session,
)
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import (
    array,
    integer,
    long,
    map,
    posexplode_array,
    posexplode_map,
    posexplode_outer_array,
    posexplode_outer_map,
    posexplode_outer_struct,
    posexplode_struct,
    string,
    struct,
)

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_collection_generator_conformance"
PACKAGE = "integration_v7_collection_generator_conformance_generated"


class Term(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class InnerDocument(Schema):
    doc_id = string(nullable=False)
    terms = array(struct(Term), contains_null=False, nullable=False)
    values = array(string(), contains_null=False, nullable=False)
    attributes = map(string(), string(), value_contains_null=True, nullable=False)


class OuterDocument(Schema):
    doc_id = string(nullable=False)
    terms = array(struct(Term), contains_null=False, nullable=True)
    values = array(string(), contains_null=True, nullable=True)
    attributes = map(string(), string(), value_contains_null=True, nullable=True)


class InnerStructValue(Schema):
    ordinal = long(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class OuterStructValue(Schema):
    ordinal = long(nullable=True)
    token = string(nullable=True)
    weight = integer(nullable=True)


class InnerScalarValue(Schema):
    ordinal = long(nullable=False)
    value = string(nullable=False)


class OuterScalarValue(Schema):
    ordinal = long(nullable=True)
    value = string(nullable=True)


class InnerMapValue(Schema):
    ordinal = long(nullable=False)
    key = string(nullable=False)
    value = string(nullable=True)


class OuterMapValue(Schema):
    ordinal = long(nullable=True)
    key = string(nullable=True)
    value = string(nullable=True)


class InnerStructResult(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class OuterStructResult(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=True)
    token = string(nullable=True)
    weight = integer(nullable=True)


class InnerScalarResult(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=False)
    value = string(nullable=False)


class OuterScalarResult(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=True)
    value = string(nullable=True)


class InnerMapResult(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=False)
    key = string(nullable=False)
    value = string(nullable=True)


class OuterMapResult(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=True)
    key = string(nullable=True)
    value = string(nullable=True)


@transform
class InnerStructGenerator(Transform):
    documents = input(InnerDocument)
    values = output(InnerStructResult)

    def expand(self, document: InnerDocument) -> InnerStructResult:
        value = posexplode_struct(document.terms, as_=InnerStructValue, scope="term")
        return InnerStructResult(
            doc_id=document.doc_id,
            ordinal=value.ordinal,
            token=value.token,
            weight=value.weight,
        )


@transform
class OuterStructGenerator(Transform):
    documents = input(OuterDocument)
    values = output(OuterStructResult)

    def expand(self, document: OuterDocument) -> OuterStructResult:
        value = posexplode_outer_struct(document.terms, as_=OuterStructValue, scope="term")
        return OuterStructResult(
            doc_id=document.doc_id,
            ordinal=value.ordinal,
            token=value.token,
            weight=value.weight,
        )


@transform
class InnerScalarGenerator(Transform):
    documents = input(InnerDocument)
    values = output(InnerScalarResult)

    def expand(self, document: InnerDocument) -> InnerScalarResult:
        value = posexplode_array(document.values, as_=InnerScalarValue, value_field="value", scope="value")
        return InnerScalarResult(doc_id=document.doc_id, ordinal=value.ordinal, value=value.value)


@transform
class OuterScalarGenerator(Transform):
    documents = input(OuterDocument)
    values = output(OuterScalarResult)

    def expand(self, document: OuterDocument) -> OuterScalarResult:
        value = posexplode_outer_array(
            document.values,
            as_=OuterScalarValue,
            value_field="value",
            scope="value",
        )
        return OuterScalarResult(doc_id=document.doc_id, ordinal=value.ordinal, value=value.value)


@transform
class InnerMapGenerator(Transform):
    documents = input(InnerDocument)
    values = output(InnerMapResult)

    def expand(self, document: InnerDocument) -> InnerMapResult:
        value = posexplode_map(
            document.attributes,
            as_=InnerMapValue,
            key_field="key",
            value_field="value",
            scope="attribute",
        )
        return InnerMapResult(
            doc_id=document.doc_id,
            ordinal=value.ordinal,
            key=value.key,
            value=value.value,
        )


@transform
class OuterMapGenerator(Transform):
    documents = input(OuterDocument)
    values = output(OuterMapResult)

    def expand(self, document: OuterDocument) -> OuterMapResult:
        value = posexplode_outer_map(
            document.attributes,
            as_=OuterMapValue,
            key_field="key",
            value_field="value",
            scope="attribute",
        )
        return OuterMapResult(
            doc_id=document.doc_id,
            ordinal=value.ordinal,
            key=value.key,
            value=value.value,
        )


def test_v7_collection_generators_preserve_null_empty_and_value_semantics(spark, tmp_path) -> None:
    transforms: tuple[type[Transform], ...] = (
        InnerStructGenerator,
        OuterStructGenerator,
        InnerScalarGenerator,
        OuterScalarGenerator,
        InnerMapGenerator,
        OuterMapGenerator,
    )
    files = render_generated_projects(
        tuple((transform_type, f"{SOURCE_MODULE}.{transform_type.__name__}") for transform_type in transforms),
        generated_package=PACKAGE,
        source_schema_modules={
            SOURCE_MODULE: [
                Term,
                InnerDocument,
                OuterDocument,
                InnerStructValue,
                OuterStructValue,
                InnerScalarValue,
                OuterScalarValue,
                InnerMapValue,
                OuterMapValue,
                InnerStructResult,
                OuterStructResult,
                InnerScalarResult,
                OuterScalarResult,
                InnerMapResult,
                OuterMapResult,
            ],
        },
    )
    assert_generated_connect_safe(files)

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_collection_generator_conformance")
        inner_documents = spark.createDataFrame(
            [
                {
                    "doc_id": "doc-1",
                    "terms": [{"token": "alpha", "weight": 2}, {"token": "beta", "weight": 3}],
                    "values": ["one", "two"],
                    "attributes": {"a": "1", "b": None},
                },
                {"doc_id": "doc-2", "terms": [], "values": [], "attributes": {}},
            ],
            generated_schemas.INNER_DOCUMENT_SCHEMA,
        )
        outer_documents = spark.createDataFrame(
            [
                {
                    "doc_id": "doc-1",
                    "terms": [{"token": "alpha", "weight": 2}],
                    "values": ["one", None],
                    "attributes": {"a": "1", "b": None},
                },
                {"doc_id": "doc-2", "terms": None, "values": None, "attributes": None},
                {"doc_id": "doc-3", "terms": [], "values": [], "attributes": {}},
            ],
            generated_schemas.OUTER_DOCUMENT_SCHEMA,
        )

        _assert_matches(InnerStructGenerator, inner_documents, _inner_struct_rows())
        _assert_matches(OuterStructGenerator, outer_documents, _outer_struct_rows())
        _assert_matches(InnerScalarGenerator, inner_documents, _inner_scalar_rows())
        _assert_matches(OuterScalarGenerator, outer_documents, _outer_scalar_rows())
        _assert_matches(InnerMapGenerator, inner_documents, _inner_map_rows())
        _assert_matches(OuterMapGenerator, outer_documents, _outer_map_rows())


def _assert_matches(transform_type: type[Transform], documents, expected: list[dict[str, object]]) -> None:
    online_outputs = assert_online_generated_parity(
        lambda: transform_type(documents=documents).run(session(documents.sparkSession, execution_mode="online")),
        lambda: transform_type(documents=documents).run(
            session(documents.sparkSession, execution_mode="generated", generated_package=PACKAGE)
        ),
    )
    values = online_outputs["values"]
    order_by = tuple(column for column in ("doc_id", "ordinal", "key", "value", "token") if column in values.columns)
    assert rows(values, *order_by) == expected


def _inner_struct_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "token": "alpha", "weight": 2},
        {"doc_id": "doc-1", "ordinal": 1, "token": "beta", "weight": 3},
    ]


def _outer_struct_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "token": "alpha", "weight": 2},
        {"doc_id": "doc-2", "ordinal": None, "token": None, "weight": None},
        {"doc_id": "doc-3", "ordinal": None, "token": None, "weight": None},
    ]


def _inner_scalar_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "value": "one"},
        {"doc_id": "doc-1", "ordinal": 1, "value": "two"},
    ]


def _outer_scalar_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "value": "one"},
        {"doc_id": "doc-1", "ordinal": 1, "value": None},
        {"doc_id": "doc-2", "ordinal": None, "value": None},
        {"doc_id": "doc-3", "ordinal": None, "value": None},
    ]


def _inner_map_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "key": "a", "value": "1"},
        {"doc_id": "doc-1", "ordinal": 1, "key": "b", "value": None},
    ]


def _outer_map_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "key": "a", "value": "1"},
        {"doc_id": "doc-1", "ordinal": 1, "key": "b", "value": None},
        {"doc_id": "doc-2", "ordinal": None, "key": None, "value": None},
        {"doc_id": "doc-3", "ordinal": None, "key": None, "value": None},
    ]
