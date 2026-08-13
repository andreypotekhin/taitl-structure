from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import array, integer, long, posexplode_struct, string, struct
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


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


class DocumentTerm(Schema):
    doc_id = string(nullable=False)
    ordinal = long(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class ExpandTerms(Transform):
    documents = input(Document)
    terms = output(DocumentTerm)

    def expand(self, document: Document) -> DocumentTerm:
        term = posexplode_struct(document.terms, as_=ExpandedTerm, scope="term")
        return DocumentTerm(
            doc_id=document.doc_id,
            ordinal=term.ordinal,
            token=term.token,
            weight=term.weight,
        )


def test_posexplode_struct_records_a_row_expanding_operation() -> None:
    lowered = _lowered()
    operation = lowered.steps[0].operations[0]

    assert operation.kind == "posexplode_struct"
    assert operation.posexplode_struct is not None
    assert operation.posexplode_struct.scope == "term"
    assert operation.posexplode_struct.schema is ExpandedTerm
    assert operation.posexplode_struct.ordinal == "ordinal"


def test_posexplode_struct_renders_public_pyspark_generator_source() -> None:
    rendered = render_pyspark_step(_lowered().steps[0], current="similarity", sources={"similarity": "similarity"})

    assert (
        'F.posexplode(F.col("document.terms")).alias("__structure_term_1_pos", "__structure_term_1_item")'
        in rendered
    )
    assert 'F.col("__structure_term_1_pos").cast(T.LongType())' in rendered
    assert 'F.col("__structure_term_1_item.token")' in rendered
    assert 'F.col("__structure_term_1_item.weight")' in rendered
    assert 'F.col("ordinal")' in rendered
    assert 'F.col("token")' in rendered


def test_posexplode_struct_explain_names_row_expansion_and_streaming_status() -> None:
    text = render_explain_report(ExpandTerms)

    assert "operations: posexplode_struct(row_multiplying scope=term schema=ExpandedTerm)" in text
    assert "STREAM-E0801: batch_only in expand (posexplode_struct term)" not in text


def test_posexplode_struct_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.ExpandTerms",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["expand.posexplode_struct[0].term"]
    assert dependency.sources == ("similarity.terms",)
    assert dependency.operation == "posexplode_struct"
    assert dependency.detail["schema"] == "ExpandedTerm"


def test_posexplode_struct_rejects_scalar_arrays() -> None:
    class ScalarArrayDocument(Schema):
        values = array(string(), contains_null=False, nullable=False)

    class BadTransform(Transform):
        documents = input(ScalarArrayDocument)
        terms = output(DocumentTerm)

        def expand(self, document: ScalarArrayDocument) -> DocumentTerm:
            term = posexplode_struct(document.values, as_=ExpandedTerm, scope="term")
            return DocumentTerm(doc_id="", ordinal=term.ordinal, token=term.token, weight=term.weight)

    with pytest.raises(TypeError, match="array<struct"):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


def test_posexplode_struct_rejects_nullable_array_elements() -> None:
    class NullableTermDocument(Schema):
        terms = array(struct(Term), contains_null=True, nullable=False)

    class BadTransform(Transform):
        documents = input(NullableTermDocument)
        terms = output(DocumentTerm)

        def expand(self, document: NullableTermDocument) -> DocumentTerm:
            term = posexplode_struct(document.terms, as_=ExpandedTerm, scope="term")
            return DocumentTerm(doc_id="", ordinal=term.ordinal, token=term.token, weight=term.weight)

    with pytest.raises(TypeError, match="contains_null=False"):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(ExpandTerms, materialize_schemas=False).lowered)
