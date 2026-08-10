"""Simple-overlap filtering from reusable document-index artifacts."""

from examples.search.schemas.filtering import DocumentFilterMatch, DocumentFilterScore
from examples.search.schemas.indexing.lexical.index import DocumentTerm
from examples.search.schemas.scoring.intermediate import QueryTerm, QueryToken
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    count_distinct,
    cross_join,
    drop_duplicates,
    group_by,
    inner_join,
    left_join,
    row_number,
    types,
    union_all,
    where,
)
from structure.plugin.pyspark.dsl.expressions import literal


class FilterOverlap(Transform):
    """Rank documents by distinct query terms before expensive lexical scoring."""

    maximum_candidates = 10000

    queries = input(SearchQuery, streaming=True)
    document_terms = input(DocumentTerm)
    document_filter_targets = input(DocumentSearchTarget, streaming=True)
    expanded_query_terms = lane(QueryTerm)
    targeted_queries = lane(SearchQuery)
    unrestricted_queries = lane(SearchQuery)
    restricted_matches = lane(DocumentFilterMatch)
    unrestricted_matches = lane(DocumentFilterMatch)
    score_policy = input(ScorePolicy)
    matched_documents = lane(DocumentFilterMatch)
    ranked_documents = lane(DocumentFilterMatch)
    document_filter_scores = output(DocumentFilterScore)

    @step(input=queries, output=expanded_query_terms)
    def expand_query_terms(self, query: SearchQuery) -> QueryTerm:
        token = QueryToken.expand(query)
        where(token.token != "")
        return QueryTerm(query_id=query.id, token=token.token)

    @step(input=[queries, document_filter_targets], output=targeted_queries)
    def select_targeted_queries(self, query: SearchQuery, target: DocumentSearchTarget) -> SearchQuery:
        inner_join(target, on=target.query_id == query.id)
        drop_duplicates(query.id)
        return SearchQuery.project(query)

    @step(input=[queries, document_filter_targets], output=unrestricted_queries)
    def select_unrestricted_queries(self, query: SearchQuery, target: DocumentSearchTarget) -> SearchQuery:
        left_join(target, on=target.query_id == query.id)
        where(target.query_id.is_null())
        return SearchQuery.project(query)

    @step(input=[expanded_query_terms, document_terms, unrestricted_queries], output=unrestricted_matches)
    def match_unrestricted_documents(
        self, query: QueryTerm, term: DocumentTerm, allowed_query: SearchQuery
    ) -> DocumentFilterMatch:
        inner_join(allowed_query, on=allowed_query.id == query.query_id)
        inner_join(on=term.term == query.token)
        zero_rank = literal(0).cast(types.long())
        group_by(query_id=query.query_id, document_id=term.document_id, filter_rank=zero_rank)
        return DocumentFilterMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            matched_terms=count_distinct(query.token),
            filter_rank=zero_rank,
        )

    @step(
        input=[expanded_query_terms, document_terms, targeted_queries, document_filter_targets],
        output=restricted_matches,
    )
    def match_restricted_documents(
        self,
        query: QueryTerm,
        term: DocumentTerm,
        allowed_query: SearchQuery,
        target: DocumentSearchTarget,
    ) -> DocumentFilterMatch:
        inner_join(allowed_query, on=allowed_query.id == query.query_id)
        inner_join(on=term.term == query.token)
        inner_join(target, on=(target.query_id == query.query_id) & (target.document_id == term.document_id))
        zero_rank = literal(0).cast(types.long())
        group_by(query_id=query.query_id, document_id=term.document_id, filter_rank=zero_rank)
        return DocumentFilterMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            matched_terms=count_distinct(query.token),
            filter_rank=zero_rank,
        )

    @step(input=[unrestricted_matches, restricted_matches], output=matched_documents)
    def merge_matches(
        self, unrestricted: DocumentFilterMatch, restricted: DocumentFilterMatch
    ) -> DocumentFilterMatch:
        merged = union_all(restricted)
        return DocumentFilterMatch.project(merged)

    @step(input=matched_documents, output=ranked_documents)
    def rank_documents(self, document: DocumentFilterMatch) -> DocumentFilterMatch:
        return DocumentFilterMatch.project(document)(
            filter_rank=row_number(
                partition_by=document.query_id,
                order_by=(document.matched_terms.desc(), document.document_id.asc()),
            )
        )

    @step(input=[ranked_documents, score_policy], output=document_filter_scores)
    def publish_filter_scores(self, document: DocumentFilterMatch, policy: ScorePolicy) -> DocumentFilterScore:
        where(document.filter_rank <= self.maximum_candidates)
        cross_join(policy, allow_cartesian=True)
        return DocumentFilterScore.project(document)(scored_at=policy.scored_at)


__all__ = ["FilterOverlap"]
