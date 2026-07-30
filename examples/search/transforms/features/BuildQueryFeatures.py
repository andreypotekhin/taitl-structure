"""Build reusable Search query feature relations."""

from examples.search.algorithms.text import normalized_token
from examples.search.schemas.features import QueryFeatures
from examples.search.schemas.features.intermediate import (
    ExpandedQueryFeatureToken,
    QueryFeatureToken,
    QueryTokenSummary,
)
from examples.search.schemas.search import SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    arr_transform,
    coalesce,
    count,
    count_distinct,
    group_by,
    left_join,
    lower,
    posexplode_struct,
    regexp_replace,
    split,
    trim,
    where,
)


class BuildQueryFeatures(Transform):
    """Build reusable lexical and caller-supplied features from search queries."""

    queries = input(SearchQuery)
    expanded_query_tokens = lane(ExpandedQueryFeatureToken)
    query_tokens = lane(QueryFeatureToken)
    query_token_summaries = lane(QueryTokenSummary)
    query_features = output(QueryFeatures)

    @step(input=queries, output=expanded_query_tokens)
    def expand_tokens(self, query: SearchQuery) -> ExpandedQueryFeatureToken:
        tokens = arr_transform(
            split(trim(query.content), pattern=r"\s+"),
            lambda value: QueryFeatureToken(query_id=query.id, token=normalized_token(value)),
        )
        token = posexplode_struct(tokens, as_=ExpandedQueryFeatureToken, scope="query_feature_token")
        where(token.token != "")
        return ExpandedQueryFeatureToken(ordinal=token.ordinal, query_id=token.query_id, token=token.token)

    @step(input=expanded_query_tokens, output=query_tokens)
    def select_tokens(self, token: ExpandedQueryFeatureToken) -> QueryFeatureToken:
        return QueryFeatureToken(query_id=token.query_id, token=token.token)

    @step(input=query_tokens, output=query_token_summaries)
    def summarize(self, token: QueryFeatureToken) -> QueryTokenSummary:
        group_by(query_id=token.query_id)
        return QueryTokenSummary(
            query_id=token.query_id,
            token_count=count(),
            distinct_token_count=count_distinct(token.token),
        )

    @step(input=[queries, query_token_summaries], output=query_features)
    def build(self, query: SearchQuery, summary: QueryTokenSummary) -> QueryFeatures:
        left_join(summary, on=summary.query_id == query.id)
        return QueryFeatures(
            query_id=query.id,
            queryset=query.queryset,
            language=query.language,
            normalized_content=lower(regexp_replace(trim(query.content), pattern=r"\s+", replacement=" ")),
            token_count=coalesce(summary.token_count, 0),
            distinct_token_count=coalesce(summary.distinct_token_count, 0),
            is_question=query.is_question,
            is_time_sensitive=query.is_time_sensitive,
        )
