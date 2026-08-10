"""Select queries observed in the recent impression window."""

from examples.search.schemas.clicks import DailyImpressions
from examples.search.schemas.search import ScorePolicy, SearchQuery
from structure import Transform, input, output, parameter, step
from structure.plugin.pyspark import (
    cross_join,
    datediff,
    drop_duplicates,
    inner_join,
    lower,
    regexp_replace,
    trim,
    where,
)


class SelectRecentQueries(Transform):
    """Select labeled queries observed during the recent scoring window."""

    queries = input(SearchQuery)
    daily_impressions = input(DailyImpressions)
    score_policy = input(ScorePolicy)
    recent_days = parameter(7)
    recent_queries = output(SearchQuery)

    @step(input=[daily_impressions, queries, score_policy], output=recent_queries)
    def select_recent_queries(
        self,
        impression: DailyImpressions,
        query: SearchQuery,
        policy: ScorePolicy,
    ) -> SearchQuery:
        impression_query = lower(regexp_replace(trim(impression.query), pattern=r"\s+", replacement=" "))
        query_text = lower(regexp_replace(trim(query.content), pattern=r"\s+", replacement=" "))
        inner_join(query, on=impression_query == query_text)
        cross_join(policy, allow_cartesian=True)
        age = datediff(policy.scored_at, impression.window.end)
        where((age >= 0) & (age <= self.recent_days))
        drop_duplicates(query.id)
        return SearchQuery.project(query)
