"""Shared reusable-index scoring inputs."""

from examples.search.schemas.indexing.lexical.index import DocumentTerm, ParagraphTerm, SectionTerm, SentenceTerm
from examples.search.schemas.scoring.intermediate import QueryTerm, QueryTermCount, QueryToken
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from structure import Transform, input, lane, step
from structure.plugin.pyspark import count, group_by, watermark, where


class ScoreBase(Transform):
    """Accept one or more queries and four reusable target-grain indexes."""

    queries = input(SearchQuery, streaming=True)
    document_terms = input(DocumentTerm)
    section_terms = input(SectionTerm)
    paragraph_terms = input(ParagraphTerm)
    sentence_terms = input(SentenceTerm)
    targets = input(DocumentSearchTarget, streaming=True)
    expanded_query_terms = lane(QueryTerm)
    query_sizes = lane(QueryTermCount)

    @step(input=queries, output=expanded_query_terms)
    def expand_query_terms(self, query: SearchQuery) -> QueryTerm:
        watermark(query.requested_at, delay="10 minutes")
        token = QueryToken.expand(query)
        where(token.token != "")
        return QueryTerm(query_id=query.id, token=token.token)

    @step(input=expanded_query_terms, output=query_sizes)
    def count_query_terms(self, query: QueryTerm) -> QueryTermCount:
        group_by(query_id=query.query_id)
        return QueryTermCount(query_id=query.query_id, query_terms=count())
