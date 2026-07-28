from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import array, explode_struct, integer, string, struct
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class Term(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class Document(Schema):
    doc_id = string(nullable=False)
    terms = array(struct(Term), contains_null=False, nullable=False)


class ExplodedTerm(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class DocumentTerm(Schema):
    doc_id = string(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class ExpandTerms(Transform):
    documents = input(Document)
    terms = output(DocumentTerm)

    def expand(self, document: Document) -> DocumentTerm:
        term = explode_struct(document.terms, as_=ExplodedTerm, scope="term")
        return DocumentTerm(
            doc_id=document.doc_id,
            token=term.token,
            weight=term.weight,
        )


def test_explode_struct_records_a_row_expanding_operation() -> None:
    operation = _lowered().steps[0].operations[0]

    assert operation.kind == "explode_struct"
    assert operation.posexplode_struct is not None
    assert operation.posexplode_struct.function == "explode"
    assert operation.posexplode_struct.scope == "term"
    assert operation.posexplode_struct.schema is ExplodedTerm
    assert operation.posexplode_struct.ordinal is None


def test_explode_struct_renders_public_pyspark_generator_source() -> None:
    rendered = render_pyspark_step(_lowered().steps[0], current="documents", sources={"documents": "documents"})

    assert 'F.explode(F.col("document.terms")).alias("__structure_term_1_item")' in rendered
    assert 'F.col("__structure_term_1_item.token")' in rendered
    assert 'F.col("__structure_term_1_item.weight")' in rendered
    assert "__structure_term_1_pos" not in rendered
    assert "posexplode" not in rendered


def test_explode_struct_explain_names_row_expansion_and_streaming_status() -> None:
    text = render_explain_report(ExpandTerms)

    assert "operations: explode_struct(row_multiplying scope=term schema=ExplodedTerm)" in text
    assert "STREAM-E0801: batch_only in expand (explode_struct term)" in text


def test_explode_struct_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.ExpandTerms",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["expand.explode_struct[0].term"]
    assert dependency.sources == ("documents.terms",)
    assert dependency.operation == "explode_struct"
    assert dependency.detail["schema"] == "ExplodedTerm"
    assert dependency.detail["ordinal"] is None


def test_explode_struct_requires_exact_element_schema() -> None:
    class BadTerm(Schema):
        token = string(nullable=False)
        weight = integer(nullable=False)
        extra = string(nullable=False)

    class BadTransform(Transform):
        documents = input(Document)
        terms = output(DocumentTerm)

        def expand(self, document: Document) -> DocumentTerm:
            term = explode_struct(document.terms, as_=BadTerm, scope="term")
            return DocumentTerm(doc_id="", token=term.token, weight=term.weight)

    with pytest.raises(TypeError, match="exactly the array element fields"):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(ExpandTerms, materialize_schemas=False).lowered)
