"""Resolve field clauses and prepare delegated document-search inputs."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.fields import (
    FieldSearchClauseMatch,
    FieldSearchDelegation,
    FieldSearchDocumentMatch,
    FieldSearchQuery,
    FieldSearchTerm,
    FieldSearchTermMatch,
    FieldTerm,
)
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    concat_ws,
    count_distinct,
    drop_duplicates,
    group_by,
    inner_join,
    max,
    sha2,
    union_all,
    where,
)


class FieldSearch(Transform):
    """Resolve metadata matches and project body queries, requests, and targets."""

    queries = input(FieldSearchQuery, streaming=True)
    query_terms = input(FieldSearchTerm, streaming=True)
    field_terms = input(FieldTerm)
    requests = input(SearchRequest, streaming=True)
    term_matches = lane(FieldSearchTermMatch)
    clause_matches = lane(FieldSearchClauseMatch)
    body_only_queries = lane(FieldSearchQuery)
    mixed_body_queries = lane(FieldSearchQuery)
    delegatable_queries = lane(FieldSearchQuery)
    document_matches = output(FieldSearchDocumentMatch)
    delegations = output(FieldSearchDelegation)
    body_queries = output(SearchQuery)
    delegated_requests = output(SearchRequest)
    document_filter_targets = output(DocumentSearchTarget)

    @step(input=[query_terms, field_terms], output=term_matches)
    def match_terms(self, query: FieldSearchTerm, field: FieldTerm) -> FieldSearchTermMatch:
        inner_join(on=(query.term == field.term) & (query.field_name == field.field_name))
        position_offset = field.position - query.term_ordinal
        group_by(
            query_id=query.query_id,
            document_id=field.document_id,
            field_name=field.field_name,
            clause_ordinal=query.clause_ordinal,
            position_offset=position_offset,
        )
        matched_term_count = count_distinct(query.term_ordinal)
        expected_term_count = max(query.term_count)
        where((query.is_phrase == False) | (field.phrase_enabled == True))
        return FieldSearchTermMatch(
            query_id=query.query_id,
            document_id=field.document_id,
            field_name=field.field_name,
            clause_ordinal=query.clause_ordinal,
            position_offset=position_offset,
            matched_term_count=matched_term_count,
            expected_term_count=expected_term_count,
        )

    @step(input=term_matches, output=clause_matches)
    def resolve_clauses(self, match: FieldSearchTermMatch) -> FieldSearchClauseMatch:
        where(match.matched_term_count == match.expected_term_count)
        group_by(
            query_id=match.query_id,
            document_id=match.document_id,
            field_name=match.field_name,
            clause_ordinal=match.clause_ordinal,
        )
        return FieldSearchClauseMatch(
            query_id=match.query_id,
            document_id=match.document_id,
            field_name=match.field_name,
            clause_ordinal=match.clause_ordinal,
        )

    @step(input=[clause_matches, queries], output=document_matches)
    def resolve_boolean(self, clause: FieldSearchClauseMatch, query: FieldSearchQuery) -> FieldSearchDocumentMatch:
        inner_join(on=(query.id == clause.query_id) & (query.clause_count > 0))
        group_by(
            query_id=clause.query_id,
            document_id=clause.document_id,
            expected_clause_count=query.clause_count,
        )
        matched_clause_count = count_distinct(clause.clause_ordinal)
        return FieldSearchDocumentMatch(
            query_id=clause.query_id,
            document_id=clause.document_id,
            matched_clause_count=matched_clause_count,
            expected_clause_count=query.clause_count,
        )

    @step(input=queries, output=body_only_queries)
    def select_body_only_queries(self, query: FieldSearchQuery) -> FieldSearchQuery:
        where(query.requires_content & (query.clause_count == 0))
        return FieldSearchQuery.project(query)

    @step(input=[queries, document_matches], output=mixed_body_queries)
    def select_mixed_body_queries(
        self, query: FieldSearchQuery, document: FieldSearchDocumentMatch
    ) -> FieldSearchQuery:
        inner_join(on=query.id == document.query_id)
        where(query.requires_content & (query.clause_count > 0))
        where((query.operator == "or") | (document.matched_clause_count == document.expected_clause_count))
        drop_duplicates(query.id)
        return FieldSearchQuery.project(query)

    @step(input=[body_only_queries, mixed_body_queries], output=delegatable_queries)
    def merge_delegatable_queries(
        self, body_only: FieldSearchQuery, mixed: FieldSearchQuery
    ) -> FieldSearchQuery:
        merged = union_all(mixed)
        return FieldSearchQuery.project(merged)

    @step(input=delegatable_queries, output=delegations)
    def build_delegations(self, query: FieldSearchQuery) -> FieldSearchDelegation:
        delegated_query_id = sha2(
            concat_ws("\x1f", "field-search-content-v1", query.id, query.content),
            bits=256,
        )
        return FieldSearchDelegation(query_id=query.id, delegated_query_id=delegated_query_id)

    @step(input=[delegatable_queries, delegations], output=body_queries)
    def build_body_queries(
        self, query: FieldSearchQuery, delegation: FieldSearchDelegation
    ) -> SearchQuery:
        inner_join(on=delegation.query_id == query.id)
        return SearchQuery(
            id=delegation.delegated_query_id,
            queryset=query.queryset,
            content=query.content,
            requested_at=query.requested_at,
            labels=query.labels,
            is_question=query.is_question,
            is_time_sensitive=query.is_time_sensitive,
            language=query.language,
        )

    @step(input=[delegatable_queries, delegations, requests], output=delegated_requests)
    def build_delegated_requests(
        self,
        query: FieldSearchQuery,
        delegation: FieldSearchDelegation,
        request: SearchRequest,
    ) -> SearchRequest:
        inner_join(on=request.query_id == query.id)
        inner_join(on=delegation.query_id == query.id)
        return SearchRequest.project(request)(
            query_id=delegation.delegated_query_id,
            query=query.content,
        )

    @step(input=[document_matches, queries, delegations], output=document_filter_targets)
    def build_document_filter_targets(
        self,
        document: FieldSearchDocumentMatch,
        query: FieldSearchQuery,
        delegation: FieldSearchDelegation,
    ) -> DocumentSearchTarget:
        inner_join(on=query.id == document.query_id)
        inner_join(on=delegation.query_id == query.id)
        where((query.operator == "or") | (document.matched_clause_count == document.expected_clause_count))
        return DocumentSearchTarget(
            query_id=delegation.delegated_query_id,
            document_id=document.document_id,
        )


__all__ = ["FieldSearch"]
