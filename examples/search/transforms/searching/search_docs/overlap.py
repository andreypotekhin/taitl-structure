"""Overlap-based document candidate narrowing."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.scoring.overlap import DocumentOverlapScore
from examples.search.schemas.search import DocumentSearchCandidate, ScorePolicy
from examples.search.transforms.searching.search_docs.admit import RetrieveDocuments
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import cross_join, datediff, drop_duplicates, inner_join, row_number, union_all, where


class OverlapDocuments(Transform):
    """Keep the best overlap candidates before feedback-aware reranking."""

    maximum_candidates = 100

    candidates = input(DocumentSearchCandidate)
    document_overlap_scores = input(DocumentOverlapScore)
    online_document_overlap_scores = input(DocumentOverlapScore)
    requests = input(SearchRequest)
    score_policy = input(ScorePolicy)
    merged_scores = lane(DocumentOverlapScore)
    ranked_candidates = lane(DocumentSearchCandidate)
    overlapped_candidates = output(DocumentSearchCandidate)

    @step(input=[document_overlap_scores, online_document_overlap_scores, requests, score_policy], output=merged_scores)
    def merge_scores(
        self,
        stored: DocumentOverlapScore,
        online: DocumentOverlapScore,
        request: SearchRequest,
        policy: ScorePolicy,
    ) -> DocumentOverlapScore:
        score: DocumentOverlapScore = union_all(online)
        inner_join(request, on=request.query_id == score.query_id)
        cross_join(policy, allow_cartesian=True)
        age = datediff(request.requested_at, score.scored_at)
        where((score.scored_at <= request.requested_at) & (age >= 0) & (age <= policy.maximum_age_days))
        drop_duplicates(score.query_id, score.document_id)
        return DocumentOverlapScore.project(score)

    @step(input=[candidates, merged_scores], output=ranked_candidates)
    def rank_candidates(
        self, candidate: DocumentSearchCandidate, overlap: DocumentOverlapScore
    ) -> DocumentSearchCandidate:
        where(candidate.candidate_rank <= RetrieveDocuments.maximum_candidates)
        inner_join(
            overlap,
            on=(overlap.query_id == candidate.search_query_id) & (overlap.document_id == candidate.document_id),
        )
        return DocumentSearchCandidate.project(candidate)(
            candidate_rank=row_number(
                partition_by=(candidate.search_query_id, candidate.user_band_id, candidate.experiment_id),
                order_by=(overlap.score_overlap.desc_nulls_last(), candidate.score.desc_nulls_last(), candidate.document_id.asc_nulls_first()),
            )
        )

    @step(input=ranked_candidates, output=overlapped_candidates)
    def select_candidates(self, candidate: DocumentSearchCandidate) -> DocumentSearchCandidate:
        where(candidate.candidate_rank <= self.maximum_candidates)
        return DocumentSearchCandidate.project(candidate)
