"""Select query groups that need online score resolution."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.scoring.intermediate import ScoreQueryAvailability
from examples.search.schemas.scoring.overlap import DocumentOverlapScore
from examples.search.schemas.search import DocumentScore, ScorePolicy, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import cross_join, datediff, drop_duplicates, inner_join, left_join, where


class SelectGapQueries(Transform):
    """Select queries without fresh document and overlap scores."""

    queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    document_scores = input(DocumentScore)
    document_overlap_scores = input(DocumentOverlapScore)
    score_policy = input(ScorePolicy)

    document_availability = lane(ScoreQueryAvailability)
    overlap_availability = lane(ScoreQueryAvailability)
    gap_queries = output(SearchQuery)

    @step(input=[document_scores, requests, score_policy], output=document_availability)
    def find_available_documents(
        self, score: DocumentScore, request: SearchRequest, policy: ScorePolicy
    ) -> ScoreQueryAvailability:
        inner_join(on=request.query_id == score.query_id)
        cross_join(policy, allow_cartesian=True)
        where(self._is_fresh(score.scored_at, request.requested_at, policy.maximum_age_days, policy.effective_at))
        drop_duplicates(score.query_id)
        return ScoreQueryAvailability(query_id=score.query_id)

    @step(input=[document_overlap_scores, requests, score_policy], output=overlap_availability)
    def find_available_overlaps(
        self, score: DocumentOverlapScore, request: SearchRequest, policy: ScorePolicy
    ) -> ScoreQueryAvailability:
        inner_join(on=request.query_id == score.query_id)
        cross_join(policy, allow_cartesian=True)
        where(self._is_fresh(score.scored_at, request.requested_at, policy.maximum_age_days, policy.effective_at))
        drop_duplicates(score.query_id)
        return ScoreQueryAvailability(query_id=score.query_id)

    @step(input=[queries, document_availability, overlap_availability], output=gap_queries)
    def select_gap_queries(
        self,
        query: SearchQuery,
        document: ScoreQueryAvailability,
        overlap: ScoreQueryAvailability,
    ) -> SearchQuery:
        left_join(document, on=query.id == document.query_id)
        left_join(overlap, on=query.id == overlap.query_id)
        where(document.query_id.is_null() | overlap.query_id.is_null())
        return SearchQuery.project(query)

    @staticmethod
    def _is_fresh(score_at: object, requested_at: object, maximum_age_days: object, effective_at: object) -> object:
        age = datediff(requested_at, score_at)
        return (score_at <= requested_at) & (score_at >= effective_at) & (age >= 0) & (age <= maximum_age_days)
