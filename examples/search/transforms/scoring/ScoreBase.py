"""Shared reusable-index scoring inputs."""

from examples.search.schemas.search import (
    DocumentIndexTerm,
    ExpandedQueryToken,
    ParagraphIndexTerm,
    QueryTerm,
    QueryTermCount,
    QueryToken,
    SearchQuery,
    SectionIndexTerm,
    SentenceIndexTerm,
)
from structure import Transform, input, lane, step
from structure.plugin.pyspark import (
    arr_transform,
    count,
    drop_duplicates,
    group_by,
    lower,
    posexplode_struct,
    regexp_replace,
    split,
    trim,
    where,
)


class ScoreBase(Transform):
    """Accept one or more queries and four reusable target-grain indexes."""

    queries = input(SearchQuery)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
    expanded_query_terms = lane(QueryTerm)
    query_terms = lane(QueryTerm)
    query_sizes = lane(QueryTermCount)

    @step(input=queries, output=expanded_query_terms)
    def expand_query_terms(self, query: SearchQuery) -> QueryTerm:
        tokens = arr_transform(
            split(trim(query.content), pattern=r"\s+"),
            lambda token: QueryToken(
                token=lower(
                    regexp_replace(
                        trim(token),
                        pattern=r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$",
                        replacement="",
                    )
                )
            ),
        )
        token = posexplode_struct(tokens, as_=ExpandedQueryToken, scope="query_token")
        where(token.token != "")
        return QueryTerm(query_id=query.id, token=token.token)

    @step(input=expanded_query_terms, output=query_terms)
    def select_distinct_query_terms(self, query: QueryTerm) -> QueryTerm:
        drop_duplicates(query.query_id, query.token)
        return QueryTerm.project(query)

    @step(input=query_terms, output=query_sizes)
    def count_query_terms(self, query: QueryTerm) -> QueryTermCount:
        group_by(query_id=query.query_id)
        return QueryTermCount(query_id=query.query_id, query_terms=count())
