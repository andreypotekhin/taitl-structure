"""Combine bounded popular and recent offline query populations."""

from examples.search.schemas.search import SearchQuery
from structure import Transform, input, output, step
from structure.plugin.pyspark import drop_duplicates, union_all


class MergeOfflineQueries(Transform):
    """Union popular queries with recent queries without duplicate query IDs."""

    popular_queries = input(SearchQuery)
    recent_queries = input(SearchQuery)
    offline_queries = output(SearchQuery)

    @step(input=[popular_queries, recent_queries], output=offline_queries)
    def merge_queries(self, popular: SearchQuery, recent: SearchQuery) -> SearchQuery:
        query: SearchQuery = union_all(recent)
        drop_duplicates(query.id)
        return SearchQuery.project(query)
