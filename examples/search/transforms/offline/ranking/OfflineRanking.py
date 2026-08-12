"""Materialize bounded vector-ranking artifacts for offline caches."""

from examples.search.schemas.indexing.vector import *
from examples.search.transforms.ranking import *
from structure import *


class OfflineRanking(Transform):
    """Rank offline vector scores and publish cacheable candidate relations."""

    policy = input(VectorIndexPolicy)
    document_scores = input(DocumentVectorScore)
    paragraph_scores = input(ParagraphVectorScore)

    ranked = Ranking(
        policy=policy,
        document_scores=document_scores,
        paragraph_scores=paragraph_scores,
    )

    document_vector_candidates = output(DocumentVectorCandidate, ranked.document_candidates)
    paragraph_vector_candidates = output(ParagraphVectorCandidate, ranked.paragraph_candidates)
