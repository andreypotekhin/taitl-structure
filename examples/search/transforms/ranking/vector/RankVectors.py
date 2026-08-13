"""Rank and bound exact vector scores."""

from examples.search.schemas.indexing.vector import *
from examples.search.transforms.lib.Vectors import *
from structure import *
from structure.plugin.pyspark import *


class RankVectors(Transform):
    """Rank same-grain vector scores deterministically and apply the candidate bound."""

    document_scores = input(DocumentVectorScore)
    paragraph_scores = input(ParagraphVectorScore)
    policy = input(VectorIndexPolicy)
    valid_policy = lane(VectorIndexPolicy)
    ranked_document_candidates = lane(DocumentVectorCandidate)
    ranked_paragraph_candidates = lane(ParagraphVectorCandidate)
    document_candidates = output(DocumentVectorCandidate)
    paragraph_candidates = output(ParagraphVectorCandidate)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: VectorIndexPolicy) -> VectorIndexPolicy:
        validated = require_all(Vectors.valid_policy(policy))
        return VectorIndexPolicy.project(validated)

    @step(input=[document_scores, valid_policy], output=ranked_document_candidates)
    def rank_documents(
        self, score: DocumentVectorScore, policy: VectorIndexPolicy
    ) -> DocumentVectorCandidate:
        param_join(policy)
        return DocumentVectorCandidate.project(score)(
            rank=row_number(
                partition_by=score.query_id,
                order_by=(score.cosine_similarity.desc_nulls_last(), score.document_id.asc_nulls_first()),
            )
        )

    @step(input=[ranked_document_candidates, valid_policy], output=document_candidates)
    def publish_documents(
        self, candidate: DocumentVectorCandidate, policy: VectorIndexPolicy
    ) -> DocumentVectorCandidate:
        param_join(policy)
        where(candidate.rank <= policy.maximum_candidates)
        return DocumentVectorCandidate.project(candidate)

    @step(input=[paragraph_scores, valid_policy], output=ranked_paragraph_candidates)
    def rank_paragraphs(
        self, score: ParagraphVectorScore, policy: VectorIndexPolicy
    ) -> ParagraphVectorCandidate:
        param_join(policy)
        return ParagraphVectorCandidate.project(score)(
            rank=row_number(
                partition_by=score.query_id,
                order_by=(
                    score.cosine_similarity.desc_nulls_last(),
                    score.document_id.asc_nulls_first(),
                    score.section_id.asc_nulls_first(),
                    score.paragraph_id.asc_nulls_first(),
                ),
            )
        )

    @step(input=[ranked_paragraph_candidates, valid_policy], output=paragraph_candidates)
    def publish_paragraphs(
        self, candidate: ParagraphVectorCandidate, policy: VectorIndexPolicy
    ) -> ParagraphVectorCandidate:
        param_join(policy)
        where(candidate.rank <= policy.maximum_candidates)
        return ParagraphVectorCandidate.project(candidate)
