"""Resolve field clauses and prepare delegated document-search inputs."""

from examples.search.schemas.fields import (
    FieldSearchClauseMatch,
    FieldSearchDocumentMatch,
    FieldSearchQuery,
    FieldSearchTerm,
    FieldSearchTermMatch,
    FieldTerm,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import count_distinct, drop_duplicates, group_by, inner_join, max, union_all, where


class FieldSearch(Transform):
    """Resolve metadata matches and project body queries, requests, and targets."""

    queries = input(FieldSearchQuery, streaming=True)
    query_terms = input(FieldSearchTerm, streaming=True)
    field_terms = input(FieldTerm)
    term_matches = lane(FieldSearchTermMatch)
    clause_matches = lane(FieldSearchClauseMatch)
    body_only_queries = lane(FieldSearchQuery)
    mixed_body_queries = lane(FieldSearchQuery)
    delegatable_queries = output(FieldSearchQuery)
    document_matches = output(FieldSearchDocumentMatch)

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
