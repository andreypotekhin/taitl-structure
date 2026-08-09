"""Metadata boolean/phrase search with optional delegation to ``LexIndex`` scores."""

from examples.search.schemas.fields import *
from examples.search.schemas.search import *
from structure import *
from structure.plugin.pyspark import *


class SearchFields(Transform):
    """Resolve field-qualified metadata clauses and intersect content scores only for ``content:``."""

    queries = input(FieldSearchQuery)
    query_terms = input(FieldSearchTerm)
    field_terms = input(FieldTerm)
    content_scores = input(DocumentScore)
    term_matches = lane(FieldSearchTermMatch)
    clause_matches = lane(FieldSearchClauseMatch)
    document_matches = lane(FieldSearchDocumentMatch)
    results = output(FieldSearchResult)

    @step(input=[query_terms, field_terms], output=term_matches)
    def match_terms(self, query: FieldSearchTerm, field: FieldTerm) -> FieldSearchTermMatch:
        inner_join(
            on=(query.term == field.term) & (query.field_name.is_null() | (query.field_name == field.field_name))
        )
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

    @step(input=[document_matches, queries], output=results)
    def publish_metadata(self, document: FieldSearchDocumentMatch, query: FieldSearchQuery) -> FieldSearchResult:
        inner_join(on=query.id == document.query_id)
        where(query.requires_content == False)
        where((query.operator == "or") | (document.matched_clause_count == document.expected_clause_count))
        return FieldSearchResult(
            query_id=document.query_id,
            document_id=document.document_id,
            score=0.0,
            match_scope="metadata",
        )

    @step(input=[queries, content_scores], output=results)
    def publish_content(self, query: FieldSearchQuery, score: DocumentScore) -> FieldSearchResult:
        inner_join(on=query.id == score.query_id)
        where(query.requires_content & (query.clause_count == 0))
        return FieldSearchResult(
            query_id=query.id,
            document_id=score.document_id,
            score=score.score,
            match_scope="content",
        )

    @step(input=[document_matches, queries, content_scores], output=results)
    def publish_mixed(
        self,
        document: FieldSearchDocumentMatch,
        query: FieldSearchQuery,
        score: DocumentScore,
    ) -> FieldSearchResult:
        inner_join(query, on=query.id == document.query_id)
        inner_join(score, on=(score.query_id == document.query_id) & (score.document_id == document.document_id))
        where(query.requires_content & (query.clause_count > 0))
        where((query.operator == "or") | (document.matched_clause_count == document.expected_clause_count))
        return FieldSearchResult(
            query_id=document.query_id,
            document_id=document.document_id,
            score=score.score,
            match_scope="metadata+content",
        )
