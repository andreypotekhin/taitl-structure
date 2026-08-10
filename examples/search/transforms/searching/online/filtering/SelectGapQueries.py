"""Select query groups that need online filter resolution."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.filtering import DocumentFilterScore, FilterQueryAvailability
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import cross_join, datediff, drop_duplicates, inner_join, left_join, where


class SelectGapQueries(Transform):
    """Select queries without a fresh persisted simple-overlap filter."""

    queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    document_filter_scores = input(DocumentFilterScore)
    document_filter_targets = input(DocumentSearchTarget, streaming=True)
    score_policy = input(ScorePolicy)

    filter_availability = lane(FilterQueryAvailability)
    targeted_queries = lane(SearchQuery)
    gap_queries = output(SearchQuery)

    @step(input=[queries, document_filter_targets], output=targeted_queries)
    def select_targeted_queries(self, query: SearchQuery, target: DocumentSearchTarget) -> SearchQuery:
        inner_join(target, on=target.query_id == query.id)
        drop_duplicates(query.id)
        return SearchQuery.project(query)

    @step(input=[document_filter_scores, requests, score_policy], output=filter_availability)
    def find_available_filters(
        self,
        score: DocumentFilterScore,
        request: SearchRequest,
        policy: ScorePolicy,
    ) -> FilterQueryAvailability:
        inner_join(request, on=request.query_id == score.query_id)
        cross_join(policy, allow_cartesian=True)
        age = datediff(request.requested_at, score.scored_at)
        where(
            (score.scored_at <= request.requested_at)
            & (score.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
        )
        drop_duplicates(score.query_id)
        return FilterQueryAvailability(query_id=score.query_id)

    @step(input=[queries, filter_availability, targeted_queries], output=gap_queries)
    def select_gap_queries(
        self,
        query: SearchQuery,
        availability: FilterQueryAvailability,
        targeted: SearchQuery,
    ) -> SearchQuery:
        left_join(availability, on=query.id == availability.query_id)
        left_join(targeted, on=query.id == targeted.id)
        where(availability.query_id.is_null() | targeted.id.is_not_null())
        return SearchQuery.project(query)


__all__ = ["SelectGapQueries"]
