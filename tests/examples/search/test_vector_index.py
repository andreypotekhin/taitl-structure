from typing import cast

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.schemas.similarity import *
from examples.search.transforms.indexing.vector import *
from examples.search.transforms.online.scoring.lexical.MergeDocumentVectorScores import MergeDocumentVectorScores
from examples.search.transforms.online.scoring.lexical.MergeParagraphVectorScores import MergeParagraphVectorScores
from examples.search.transforms.online.scoring.lexical.OnlineScoring import OnlineScoring
from examples.search.transforms.ranking import *
from examples.search.transforms.ranking.vector import *
from examples.search.transforms.scoring.similarity import *
from examples.search.transforms.scoring.vector import *
from examples.search.transforms.searching.search_docs.fusion import *
from examples.search.transforms.searching.search_docs.SearchDocuments import *
from examples.search.transforms.searching.search_similarity import *
from examples.search.transforms.vectorization import *
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def test_vector_contract_declares_non_null_double_arrays_and_policy_fields() -> None:
    vector = DocumentVectorEmbedding._structure_fields["vector"]
    assert vector.nullable is False
    assert vector.type.name == "array"
    assert vector.type.element.name == "double"
    assert vector.type.contains_null is False
    assert VectorIndexPolicy._structure_fields["rrf_k"].nullable is False
    assert VectorIndexPolicy._structure_fields["maximum_candidates"].nullable is False
    assert DocumentVectorIndexSummary._structure_fields["target_count"].type.name == "long"
    assert SimilarityFusionPolicy._structure_fields["maximum_results"].nullable is False
    assert HybridIndexedSimilarDocument._structure_fields["rrf_score"].nullable is False
    assert HybridIndexedSimilarDocument._structure_fields["vector_backend"].nullable is True
    assert DocumentSearchCandidate._structure_fields["vector_rank"].nullable is True
    assert DocumentSearchResult._structure_fields["rrf_score"].nullable is False
    assert DocumentVectorScore._structure_fields["query_document_id"].nullable is True
    assert DocumentVectorScore._structure_fields["scored_at"].nullable is False


def test_vector_index_and_scoring_transforms_compile() -> None:
    Compiler.frontend.compile()(VectorIndex, materialize_schemas=False)
    Compiler.frontend.compile()(ScoreVectors, materialize_schemas=False)
    Compiler.frontend.compile()(VectorizeSearchQueries, materialize_schemas=False)
    Compiler.frontend.compile()(VectorizeSimilarityDocumentQueries, materialize_schemas=False)
    Compiler.frontend.compile()(ScoreDocumentVectors, materialize_schemas=False)
    Compiler.frontend.compile()(ScoreParagraphVectors, materialize_schemas=False)
    Compiler.frontend.compile()(RankVectors, materialize_schemas=False)
    Compiler.frontend.compile()(MergeDocumentVectorScores, materialize_schemas=False)
    Compiler.frontend.compile()(MergeParagraphVectorScores, materialize_schemas=False)
    Compiler.frontend.compile()(FuseSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptLexicalSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptLexicalParagraphs, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptVectorSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptVectorParagraphs, materialize_schemas=False)
    Compiler.frontend.compile()(RerankSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(SearchSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(SearchSimilarityParagraphs, materialize_schemas=False)
    Compiler.frontend.compile()(FuseDocumentCandidates, materialize_schemas=False)
    Compiler.frontend.compile()(SearchDocuments, materialize_schemas=False)


def test_online_vector_score_merge_invalidates_groups_before_deduplication() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(MergeDocumentVectorScores, materialize_schemas=False).analysis)

    assert [step.name for step in plan.steps] == [
        "identify_invalidated_queries",
        "select_cached_scores",
        "select_online_scores",
        "merge_scores",
    ]
    cached_body = cast(PySparkStepBody, plan.steps[1].plugin_body)
    merged_body = cast(PySparkStepBody, plan.steps[-1].plugin_body)
    assert [operation.kind for operation in cached_body.operations] == [
        "join",
        "join",
        "join",
        "join",
        "join",
        "filter",
        "drop_duplicates",
    ]
    assert [operation.kind for operation in merged_body.operations] == ["union_all", "drop_duplicates"]


def test_online_scoring_publishes_neutral_merged_score_outputs() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(OnlineScoring, materialize_schemas=False).analysis)

    assert [output.name for output in plan.outputs] == [
        "document_scores",
        "section_scores",
        "paragraph_scores",
        "sentence_scores",
        "document_overlap_scores",
        "section_overlap_scores",
        "paragraph_overlap_scores",
        "sentence_overlap_scores",
        "document_vector_scores",
        "paragraph_vector_scores",
    ]
