from typing import cast

from examples.search.transforms.inference import (
    Inference,
    PublishDocumentInference,
    PublishQueryInference,
    ValidateInferencePolicy,
)
from examples.search.transforms.offline.vectorization import OfflineVectorization
from examples.search.transforms.online.filtering import SelectGapQueries as SelectFilterGaps
from examples.search.transforms.online.scoring.lexical import OnlineScoring
from examples.search.transforms.online.vectorization import OnlineVectorization
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments
from examples.search.transforms.searching.search_fields.SearchFields import SearchFields
from examples.search.transforms.vectorization import Vectorization
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import TransformPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def test_inference_and_vectorization_facets_compile() -> None:
    for transform in (Inference, Vectorization, OfflineVectorization, OnlineVectorization):
        Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_inference_publication_validates_adapter_embeddings() -> None:
    for transform in (PublishQueryInference, PublishDocumentInference):
        plan = cast(TransformPlan, Compiler.frontend.compile()(transform, materialize_schemas=False).analysis)
        body = cast(PySparkStepBody, plan.steps[0].plugin_body)
        assert any(operation.kind == "require_all" for operation in body.operations)


def test_inference_validates_policy_identity_and_dimension() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(ValidateInferencePolicy, materialize_schemas=False).analysis)
    assert plan.steps[0].name == "validate"
    body = cast(PySparkStepBody, plan.steps[0].plugin_body)
    assert any(operation.kind == "require_all" for operation in body.operations)


def test_vectorization_facets_select_the_expected_execution_mode() -> None:
    assert OfflineVectorization.vectorized.streaming_mode is False
    assert OnlineVectorization.vectorized.streaming_mode is True


def test_search_documents_accepts_optional_extra_filter_targets() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(SearchDocuments, materialize_schemas=False).analysis)
    target = next(input for input in plan.inputs if input.name == "document_filter_targets")
    assert target.optional is True


def test_online_filter_gap_selection_accepts_optional_extra_filter_targets() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(SelectFilterGaps, materialize_schemas=False).analysis)
    target = next(input for input in plan.inputs if input.name == "document_filter_targets")
    assert target.optional is True


def test_document_search_does_not_declare_non_document_score_inputs() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(SearchDocuments, materialize_schemas=False).analysis)
    declared = {input.name for input in plan.inputs}
    assert not declared.intersection(
        {
            "section_terms",
            "paragraph_terms",
            "sentence_terms",
            "section_summary",
            "paragraph_summary",
            "sentence_summary",
            "paragraph_vector_queries",
            "paragraph_vector_index",
            "paragraph_vector_scores",
        }
    )
    assert all(input.name.startswith("__optional_") for input in plan.internal_inputs)


def test_field_search_does_not_declare_non_document_score_inputs() -> None:
    declared = set(SearchFields._structure_inputs)
    assert not declared.intersection(
        {
            "paragraph_vector_scores",
            "section_terms",
            "paragraph_terms",
            "sentence_terms",
            "section_summary",
            "paragraph_summary",
            "sentence_summary",
            "paragraph_vector_queries",
            "paragraph_vector_index",
        }
    )


def test_online_scoring_exposes_gap_policy() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(OnlineScoring, materialize_schemas=False).analysis)
    gap_policy = next(input for input in plan.inputs if input.name == "gap_policy")
    assert gap_policy.optional is False
