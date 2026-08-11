from examples.search.schemas.indexing.vector import (
    DocumentVectorEmbedding,
    DocumentVectorIndexSummary,
    VectorIndexPolicy,
)
from examples.search.schemas.similarity import HybridIndexedSimilarDocument, SimilarityFusionPolicy
from examples.search.transforms.indexing.vector import VectorIndex
from examples.search.transforms.ranking.vector import RankVectors
from examples.search.transforms.scoring.vector import ScoreVectors
from examples.search.transforms.searching.search_similarity import (
    AdoptLexicalParagraphs,
    AdoptLexicalSimilarity,
    AdoptVectorParagraphs,
    AdoptVectorSimilarity,
    FuseSimilarity,
    RerankSimilarity,
    SearchSimilarity,
    SearchSimilarityParagraphs,
)
from structure.core.compiler.api import Compiler


def test_vector_contract_declares_non_null_double_arrays_and_policy_fields() -> None:
    vector = DocumentVectorEmbedding._structure_fields["vector"]
    assert vector.nullable is False
    assert vector.type.name == "array"
    assert vector.type.element.name == "double"
    assert vector.type.contains_null is False
    assert VectorIndexPolicy._structure_fields["rrf_k"].nullable is False
    assert DocumentVectorIndexSummary._structure_fields["target_count"].type.name == "long"
    assert SimilarityFusionPolicy._structure_fields["maximum_results"].nullable is False
    assert HybridIndexedSimilarDocument._structure_fields["rrf_score"].nullable is False
    assert HybridIndexedSimilarDocument._structure_fields["vector_backend"].nullable is True


def test_vector_index_and_scoring_transforms_compile() -> None:
    Compiler.frontend.compile()(VectorIndex, materialize_schemas=False)
    Compiler.frontend.compile()(ScoreVectors, materialize_schemas=False)
    Compiler.frontend.compile()(RankVectors, materialize_schemas=False)
    Compiler.frontend.compile()(FuseSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptLexicalSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptLexicalParagraphs, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptVectorSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(AdoptVectorParagraphs, materialize_schemas=False)
    Compiler.frontend.compile()(RerankSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(SearchSimilarity, materialize_schemas=False)
    Compiler.frontend.compile()(SearchSimilarityParagraphs, materialize_schemas=False)
