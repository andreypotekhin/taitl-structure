from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_projects, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import (
    array,
    explode_outer_struct,
    explode_struct,
    inline_outer_struct,
    inline_struct,
    integer,
    long,
    posexplode_outer_struct,
    posexplode_struct,
    string,
    struct,
)

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_struct_generators"
PACKAGE = "integration_v7_struct_generators_generated"


class Term(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class Document(Schema):
    doc_id = string(nullable=False)
    terms = array(struct(Term), contains_null=False, nullable=False)


class ExpandedTerm(Schema):
    ordinal = long(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class OuterExpandedTerm(Schema):
    ordinal = long(nullable=True)
    token = string(nullable=True)
    weight = integer(nullable=True)


class GeneratedTerm(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class OuterGeneratedTerm(Schema):
    token = string(nullable=True)
    weight = integer(nullable=True)


class DocumentTerm(Schema):
    doc_id = string(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class PositionedDocumentTerm(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class OuterDocumentTerm(Schema):
    doc_id = string(nullable=False)
    token = string(nullable=True)
    weight = integer(nullable=True)


class OuterPositionedDocumentTerm(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=True)
    token = string(nullable=True)
    weight = integer(nullable=True)


@transform
class PosexplodeTerms(Transform):
    documents = input(Document)
    terms = output(PositionedDocumentTerm)

    def expand(self, document: Document) -> PositionedDocumentTerm:
        term = posexplode_struct(document.terms, as_=ExpandedTerm, scope="term")
        return PositionedDocumentTerm(
            doc_id=document.doc_id,
            ordinal=term.ordinal,
            token=term.token,
            weight=term.weight,
        )


@transform
class PosexplodeOuterTerms(Transform):
    documents = input(Document)
    terms = output(OuterPositionedDocumentTerm)

    def expand(self, document: Document) -> OuterPositionedDocumentTerm:
        term = posexplode_outer_struct(document.terms, as_=OuterExpandedTerm, scope="term")
        return OuterPositionedDocumentTerm(
            doc_id=document.doc_id,
            ordinal=term.ordinal,
            token=term.token,
            weight=term.weight,
        )


@transform
class ExplodeTerms(Transform):
    documents = input(Document)
    terms = output(DocumentTerm)

    def expand(self, document: Document) -> DocumentTerm:
        term = explode_struct(document.terms, as_=GeneratedTerm, scope="term")
        return DocumentTerm(doc_id=document.doc_id, token=term.token, weight=term.weight)


@transform
class ExplodeOuterTerms(Transform):
    documents = input(Document)
    terms = output(OuterDocumentTerm)

    def expand(self, document: Document) -> OuterDocumentTerm:
        term = explode_outer_struct(document.terms, as_=OuterGeneratedTerm, scope="term")
        return OuterDocumentTerm(doc_id=document.doc_id, token=term.token, weight=term.weight)


@transform
class InlineTerms(Transform):
    documents = input(Document)
    terms = output(DocumentTerm)

    def expand(self, document: Document) -> DocumentTerm:
        term = inline_struct(document.terms, as_=GeneratedTerm, scope="term")
        return DocumentTerm(doc_id=document.doc_id, token=term.token, weight=term.weight)


@transform
class InlineOuterTerms(Transform):
    documents = input(Document)
    terms = output(OuterDocumentTerm)

    def expand(self, document: Document) -> OuterDocumentTerm:
        term = inline_outer_struct(document.terms, as_=OuterGeneratedTerm, scope="term")
        return OuterDocumentTerm(doc_id=document.doc_id, token=term.token, weight=term.weight)


def test_v7_struct_generators_match_generated_execution_on_live_backend(spark, tmp_path) -> None:
    transforms: tuple[type[Transform], ...] = (
        PosexplodeTerms,
        PosexplodeOuterTerms,
        ExplodeTerms,
        ExplodeOuterTerms,
        InlineTerms,
        InlineOuterTerms,
    )
    files = render_generated_projects(
        tuple((transform_type, f"{SOURCE_MODULE}.{transform_type.__name__}") for transform_type in transforms),
        generated_package=PACKAGE,
        source_schema_modules={
            SOURCE_MODULE: [
                Term,
                Document,
                ExpandedTerm,
                OuterExpandedTerm,
                GeneratedTerm,
                OuterGeneratedTerm,
                DocumentTerm,
                PositionedDocumentTerm,
                OuterDocumentTerm,
                OuterPositionedDocumentTerm,
            ],
        },
    )

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_struct_generators")
        documents = spark.createDataFrame(
            [
                {"doc_id": "doc-1", "terms": [{"token": "alpha", "weight": 2}, {"token": "beta", "weight": 3}]},
                {"doc_id": "doc-2", "terms": []},
                {"doc_id": "doc-3", "terms": [{"token": "gamma", "weight": 5}]},
            ],
            generated_schemas.DOCUMENT_SCHEMA,
        )

        _assert_matches(PosexplodeTerms, documents, _positioned_rows())
        _assert_matches(PosexplodeOuterTerms, documents, _outer_positioned_rows())
        _assert_matches(ExplodeTerms, documents, _plain_rows())
        _assert_matches(ExplodeOuterTerms, documents, _outer_plain_rows())
        _assert_matches(InlineTerms, documents, _plain_rows())
        _assert_matches(InlineOuterTerms, documents, _outer_plain_rows())


def _assert_matches(
    transform_type: type[Transform],
    documents,
    expected: list[dict[str, object]],
) -> None:
    assert_online_generated_parity(
        lambda: transform_type(documents=documents).run(session(documents.sparkSession, execution_mode="online")),
        lambda: transform_type(documents=documents).run(
            session(documents.sparkSession, execution_mode="generated", generated_package=PACKAGE)
        ),
    )

    online = transform_type(documents=documents).run(session(documents.sparkSession, execution_mode="online"))
    order_by = ("doc_id", "ordinal", "token") if "ordinal" in online.terms.columns else ("doc_id", "token")
    assert rows(online.terms, *order_by) == expected


def _plain_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "token": "alpha", "weight": 2},
        {"doc_id": "doc-1", "token": "beta", "weight": 3},
        {"doc_id": "doc-3", "token": "gamma", "weight": 5},
    ]


def _outer_plain_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "token": "alpha", "weight": 2},
        {"doc_id": "doc-1", "token": "beta", "weight": 3},
        {"doc_id": "doc-2", "token": None, "weight": None},
        {"doc_id": "doc-3", "token": "gamma", "weight": 5},
    ]


def _positioned_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "token": "alpha", "weight": 2},
        {"doc_id": "doc-1", "ordinal": 1, "token": "beta", "weight": 3},
        {"doc_id": "doc-3", "ordinal": 0, "token": "gamma", "weight": 5},
    ]


def _outer_positioned_rows() -> list[dict[str, object]]:
    return [
        {"doc_id": "doc-1", "ordinal": 0, "token": "alpha", "weight": 2},
        {"doc_id": "doc-1", "ordinal": 1, "token": "beta", "weight": 3},
        {"doc_id": "doc-2", "ordinal": None, "token": None, "weight": None},
        {"doc_id": "doc-3", "ordinal": 0, "token": "gamma", "weight": 5},
    ]
