"""Bound offline scoring to the most frequently requested queries."""

from examples.search.schemas.clicks import DailyImpressions
from examples.search.schemas.scoring.intermediate import PopularQueryCandidate
from examples.search.schemas.search import QueryPopularity, SearchQuery
from structure import Transform, input, lane, output, parameter, step
from structure.plugin.pyspark import coalesce, group_by, left_join, lower, regexp_replace, row_number
from structure.plugin.pyspark import sum as sum_
from structure.plugin.pyspark import trim, where
from structure.plugin.pyspark.dsl.expressions import literal


class SelectPopularQueries(Transform):
    """Select a bounded, deterministic offline query population."""

    queries = input(SearchQuery)
    daily_impressions = input(DailyImpressions)
    popularities = lane(QueryPopularity)
    ranked_queries = lane(PopularQueryCandidate)
    selected_queries = output(SearchQuery)
    maximum_queries = parameter(1000)

    @step(input=daily_impressions, output=popularities)
    def summarize_popularity(self, impression: DailyImpressions) -> QueryPopularity:
        query_key = lower(regexp_replace(trim(impression.query), pattern=r"\s+", replacement=" "))
        group_by(query=query_key)
        return QueryPopularity(query=query_key, impression_count=sum_(impression.impression_count))

    @step(input=[queries, popularities], output=ranked_queries)
    def rank_queries(self, query: SearchQuery, popularity: QueryPopularity) -> PopularQueryCandidate:
        query_key = lower(regexp_replace(trim(query.content), pattern=r"\s+", replacement=" "))
        left_join(popularity, on=popularity.query == query_key)
        impression_count = coalesce(popularity.impression_count, literal(0))
        return PopularQueryCandidate.project(query)(
            impression_count=impression_count,
            popularity_rank=row_number(
                partition_by=literal(1),
                order_by=(
                    impression_count.desc_nulls_last(),
                    query_key.asc_nulls_first(),
                    query.id.asc_nulls_first(),
                ),
            ),
        )

    @step(input=ranked_queries, output=selected_queries)
    def select_queries(self, query: PopularQueryCandidate) -> SearchQuery:
        where(query.popularity_rank <= self.maximum_queries)
        return SearchQuery.project(query)
