"""Rank online vector scores for Search workflows."""

from examples.search.schemas.indexing.vector import *
from examples.search.transforms.ranking import *
from structure import *


class OnlineRanking(Transform):
    """Rank online vector scores and publish serving candidates."""

    policy = input(VectorIndexPolicy)
    document_scores = input(DocumentVectorScore)
    paragraph_scores = input(ParagraphVectorScore)

    ranked = Ranking(
        policy=policy,
        document_scores=document_scores,
        paragraph_scores=paragraph_scores,
    )

    online_document_vector_candidates = output(DocumentVectorCandidate, ranked.document_candidates)
    online_paragraph_vector_candidates = output(ParagraphVectorCandidate, ranked.paragraph_candidates)
