"""Select query groups that need online filter resolution."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.filtering import DocumentFilterScore, FilterQueryAvailability
from examples.search.schemas.search import ScorePolicy, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import datediff, drop_duplicates, inner_join, left_join, param_join, where


class SelectGapQueries(Transform):
    """Select queries without a fresh persisted simple-overlap filter."""

    queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    document_filter_scores = input(DocumentFilterScore)
    score_policy = input(ScorePolicy)

    filter_availability = lane(FilterQueryAvailability)
    gap_queries = output(SearchQuery)

    @step(input=[document_filter_scores, requests, score_policy], output=filter_availability)
    def find_available_filters(
        self,
        score: DocumentFilterScore,
        request: SearchRequest,
        policy: ScorePolicy,
    ) -> FilterQueryAvailability:
        inner_join(request, on=request.query_id == score.query_id)
        param_join(policy)
        age = datediff(request.requested_at, score.scored_at)
        where(
            (score.scored_at <= request.requested_at)
            & (score.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
        )
        drop_duplicates(score.query_id)
        return FilterQueryAvailability(query_id=score.query_id)

    @step(input=[queries, filter_availability], output=gap_queries)
    def select_gap_queries(
        self,
        query: SearchQuery,
        availability: FilterQueryAvailability,
    ) -> SearchQuery:
        left_join(availability, on=query.id == availability.query_id)
        where(availability.query_id.is_null())
        return SearchQuery.project(query)
