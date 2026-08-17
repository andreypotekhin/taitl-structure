"""Produce the bundled exact similarity candidate relation."""

from examples.search.schemas.indexing.vector import (
    DocumentVectorCandidate,
    DocumentVectorIndex,
    SimilarityQueryEmbedding,
    VectorIndexPolicy,
)
from examples.search.schemas.search import ScorePolicy
from examples.search.schemas.similarity import SimilaritySearchQuery
from examples.search.transforms.ranking.vector import RankVectors
from examples.search.transforms.scoring.similarity import ScoreDocumentVectors
from examples.search.transforms.vectorization import VectorizeSimilarityQueries
from structure import Transform, input, output


class ExactSimilarityCandidates(Transform):
    """Build ranked document candidates with the portable exact reference backend."""

    query = input(SimilaritySearchQuery)
    query_vector_embeddings = input(SimilarityQueryEmbedding)
    document_vector_index = input(DocumentVectorIndex)
    score_policy = input(ScorePolicy)
    vector_policy = input(VectorIndexPolicy)
    document_candidates = output(DocumentVectorCandidate)

    vectorized = VectorizeSimilarityQueries(
        queries=query,
        embeddings=query_vector_embeddings,
    )

    scored = ScoreDocumentVectors(
        score_policy=score_policy,
        queries=vectorized.vector_queries,
        document_index=document_vector_index,
        policy=vector_policy,
    )

    ranked = RankVectors(
        document_scores=scored.document_scores,
        policy=vector_policy,
    )

    document_candidates = output(DocumentVectorCandidate, ranked.document_candidates)
