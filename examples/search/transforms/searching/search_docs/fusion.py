"""Rank, fuse, and bound lexical and vector document candidates."""

from examples.search.schemas.indexing.vector import VectorIndexPolicy
from examples.search.schemas.search import DocumentSearchCandidate
from examples.search.transforms.lib.Rrf import Rrf
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import group_by, max, param_join, row_number, union_all, when, where
from structure.plugin.pyspark.dsl.expressions import literal


class FuseDocumentCandidates(Transform):
    """Rank lexical candidates, fuse retrieval lanes, and bound the result set."""

    lexical_candidates = input(DocumentSearchCandidate)
    vector_candidates = input(DocumentSearchCandidate)
    policy = input(VectorIndexPolicy)
    ranked_lexical_candidates = lane(DocumentSearchCandidate)
    merged_candidates = lane(DocumentSearchCandidate)
    fused_candidates = lane(DocumentSearchCandidate)
    scored_candidates = lane(DocumentSearchCandidate)
    ranked_candidates = lane(DocumentSearchCandidate)
    candidates = output(DocumentSearchCandidate)

    @step(input=lexical_candidates, output=ranked_lexical_candidates)
    def rank_lexical_candidates(
        self, candidate: DocumentSearchCandidate
    ) -> DocumentSearchCandidate:
        rank = row_number(
            partition_by=(candidate.search_query_id, candidate.user_band_id, candidate.experiment_id),
            order_by=(candidate.score.desc_nulls_last(), candidate.document_id.asc_nulls_first()),
        )
        return DocumentSearchCandidate.project(candidate)(
            candidate_rank=rank,
            lexical_rank=rank,
            retrieval_score=candidate.score,
        )

    @step(input=[ranked_lexical_candidates, vector_candidates], output=merged_candidates)
    def merge_candidates(
        self, lexical: DocumentSearchCandidate, vector: DocumentSearchCandidate
    ) -> DocumentSearchCandidate:
        merged = union_all(vector)
        return DocumentSearchCandidate.project(merged)

    @step(input=[merged_candidates, policy], output=fused_candidates)
    def fuse_candidates(
        self, candidate: DocumentSearchCandidate, policy: VectorIndexPolicy
    ) -> DocumentSearchCandidate:
        param_join(policy)
        group_by(
            search_query_id=candidate.search_query_id,
            user_band_id=candidate.user_band_id,
            experiment_id=candidate.experiment_id,
            document_id=candidate.document_id,
            candidate_rank=literal(0),
            rrf_k=policy.rrf_k,
        )
        return DocumentSearchCandidate(
            search_query_id=candidate.search_query_id,
            experiment_id=candidate.experiment_id,
            user_band_id=candidate.user_band_id,
            band_id=max(candidate.band_id),
            query=max(candidate.query),
            candidate_rank=candidate.candidate_rank,
            document_id=candidate.document_id,
            title=max(candidate.title),
            url=max(candidate.url),
            score=max(candidate.score),
            retrieval_score=0.0,
            score_feedback=max(candidate.score_feedback),
            score_rank=max(candidate.score_rank),
            score_weight=max(candidate.score_weight),
            feedback_weight=max(candidate.feedback_weight),
            lexical_rank=max(candidate.lexical_rank),
            vector_rank=max(candidate.vector_rank),
            vector_similarity=max(candidate.vector_similarity),
            rrf_score=0.0,
            rrf_k=policy.rrf_k,
            vector_backend=max(candidate.vector_backend),
        )

    @step(input=[fused_candidates, policy], output=scored_candidates)
    def score_candidates(
        self, candidate: DocumentSearchCandidate, policy: VectorIndexPolicy
    ) -> DocumentSearchCandidate:
        param_join(policy)
        rrf_score = Rrf.score(candidate.lexical_rank, candidate.vector_rank, policy.rrf_k)
        return DocumentSearchCandidate.project(candidate)(
            retrieval_score=when(candidate.vector_rank.is_not_null(), rrf_score).otherwise(candidate.score),
            rrf_score=rrf_score,
            rrf_k=policy.rrf_k,
        )

    @step(input=[scored_candidates, policy], output=ranked_candidates)
    def rank_candidates(
        self, candidate: DocumentSearchCandidate, policy: VectorIndexPolicy
    ) -> DocumentSearchCandidate:
        param_join(policy)
        where(candidate.retrieval_score.is_not_null())
        return DocumentSearchCandidate.project(candidate)(
            candidate_rank=row_number(
                partition_by=(candidate.search_query_id, candidate.user_band_id, candidate.experiment_id),
                order_by=(
                    candidate.retrieval_score.desc_nulls_last(),
                    candidate.vector_similarity.desc_nulls_last(),
                    candidate.score.desc_nulls_last(),
                    candidate.document_id.asc_nulls_first(),
                ),
            )
        )

    @step(input=[ranked_candidates, policy], output=candidates)
    def select_candidates(
        self, candidate: DocumentSearchCandidate, policy: VectorIndexPolicy
    ) -> DocumentSearchCandidate:
        param_join(policy)
        where(candidate.candidate_rank <= policy.maximum_candidates)
        return DocumentSearchCandidate.project(candidate)
