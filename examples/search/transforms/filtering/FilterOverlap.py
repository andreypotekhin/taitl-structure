"""Simple-overlap filtering from reusable document-index artifacts."""

from examples.search.adoption import SEARCH_STREAMING_CONTRACTS_ENABLED
from examples.search.schemas.filtering import DocumentFilterMatch, DocumentFilterScore
from examples.search.schemas.indexing.lexical.index import DocumentIndexTerm
from examples.search.schemas.scoring.intermediate import QueryTerm, QueryToken
from examples.search.schemas.search import ScorePolicy, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import count_distinct, cross_join, group_by, inner_join, row_number, where
from structure.plugin.pyspark.dsl.expressions import literal


class FilterOverlap(Transform):
    """Rank documents by distinct query terms before expensive lexical scoring."""

    maximum_candidates = 10000

    queries = input(SearchQuery, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    document_terms = input(DocumentIndexTerm)
    expanded_query_terms = lane(QueryTerm)
    query_terms = lane(QueryTerm)
    score_policy = input(ScorePolicy)
    matched_documents = lane(DocumentFilterMatch)
    ranked_documents = lane(DocumentFilterMatch)
    document_filter_scores = output(DocumentFilterScore)

    @step(input=queries, output=expanded_query_terms)
    def expand_query_terms(self, query: SearchQuery) -> QueryTerm:
        token = QueryToken.expand(query)
        where(token.token != "")
        return QueryTerm(query_id=query.id, token=token.token)

    @step(input=expanded_query_terms, output=query_terms)
    def select_distinct_query_terms(self, query: QueryTerm) -> QueryTerm:
        return QueryTerm.project(query)

    @step(input=[query_terms, document_terms], output=matched_documents)
    def match_documents(self, query: QueryTerm, term: DocumentIndexTerm) -> DocumentFilterMatch:
        inner_join(on=term.token == query.token)
        group_by(query_id=query.query_id, document_id=term.document_id, filter_rank=literal(0))
        return DocumentFilterMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            matched_terms=count_distinct(query.token),
            filter_rank=literal(0),
        )

    @step(input=matched_documents, output=ranked_documents)
    def rank_documents(self, document: DocumentFilterMatch) -> DocumentFilterMatch:
        return DocumentFilterMatch.project(document)(
            filter_rank=row_number(
                partition_by=document.query_id,
                order_by=(document.matched_terms.desc(), document.document_id.asc()),
            )
        )

    @step(input=[ranked_documents, score_policy], output=document_filter_scores)
    def publish_filter_scores(
        self, document: DocumentFilterMatch, policy: ScorePolicy
    ) -> DocumentFilterScore:
        where(document.filter_rank <= self.maximum_candidates)
        cross_join(policy, allow_cartesian=True)
        return DocumentFilterScore.project(document)(scored_at=policy.scored_at)


__all__ = ["FilterOverlap"]
