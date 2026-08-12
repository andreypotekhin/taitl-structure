"""Production vector ranking composition."""

from examples.search.schemas.indexing.vector import *
from examples.search.transforms.ranking.vector import *
from structure import *


class Ranking(Transform):
    """Rank raw vector scores and publish bounded candidate relations."""

    policy = input(VectorIndexPolicy)
    document_scores = input(DocumentVectorScore)
    paragraph_scores = input(ParagraphVectorScore)

    vectors = RankVectors(
        policy=policy,
        document_scores=document_scores,
        paragraph_scores=paragraph_scores,
    )

    document_candidates = output(DocumentVectorCandidate, vectors.document_candidates)
    paragraph_candidates = output(ParagraphVectorCandidate, vectors.paragraph_candidates)
