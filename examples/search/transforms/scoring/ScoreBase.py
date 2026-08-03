"""Shared reusable-index scoring inputs."""

from examples.search.algorithms.text import normalized_token
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexTerm,
    ParagraphIndexTerm,
    SectionIndexTerm,
    SentenceIndexTerm,
)
from examples.search.schemas.scoring.intermediate import ExpandedQueryToken, QueryTerm, QueryTermCount, QueryToken
from examples.search.schemas.search import SearchQuery
from structure import Transform, input, lane, step
from structure.plugin.pyspark import (
    arr_distinct,
    arr_transform,
    count,
    group_by,
    posexplode_struct,
    split,
    trim,
    watermark,
    where,
)


class ScoreBase(Transform):
    """Accept one or more queries and four reusable target-grain indexes."""

    queries = input(SearchQuery, streaming=True)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
    expanded_query_terms = lane(QueryTerm)
    query_terms = lane(QueryTerm)
    query_sizes = lane(QueryTermCount)

    @step(input=queries, output=expanded_query_terms)
    def expand_query_terms(self, query: SearchQuery) -> QueryTerm:
        watermark(query.requested_at, delay="10 minutes")
        tokens = arr_transform(
            arr_distinct(split(trim(query.content), pattern=r"\s+")),
            lambda token: QueryToken(
                token=normalized_token(token)
            ),
        )
        token = posexplode_struct(tokens, as_=ExpandedQueryToken, scope="query_token")
        where(token.token != "")
        return QueryTerm(query_id=query.id, token=token.token)

    @step(input=expanded_query_terms, output=query_terms)
    def select_distinct_query_terms(self, query: QueryTerm) -> QueryTerm:
        return QueryTerm.project(query)

    @step(input=query_terms, output=query_sizes)
    def count_query_terms(self, query: QueryTerm) -> QueryTermCount:
        group_by(query_id=query.query_id)
        return QueryTermCount(query_id=query.query_id, query_terms=count())
