"""Schemas for document-field indexing and field-aware search."""

from examples.search.schemas.search import DocumentSearchResult
from structure import Schema
from structure.plugin.pyspark import *


class DocumentField(Schema):
    """One flattened string field supplied by a document."""

    document_id = string(nullable=False)
    field_name = string(nullable=False)
    field_value = string(nullable=False)
    field_kind = string(nullable=False)
    analyzer_policy = string(nullable=False)
    ordinal = long(nullable=False)


class FieldProfile(Schema):
    """Search behavior for one named field, or ``*`` for dynamic fields."""

    field_name = string(nullable=False)
    field_kind = string(nullable=False)
    analyzer_policy = string(nullable=False)
    phrase_enabled = boolean(nullable=False)
    searchable = boolean(nullable=False)


class AnalyzerPolicy(Schema):
    """Versioned metadata analyzer rules shared by field profiles."""

    policy_id = string(nullable=False)
    version = string(nullable=False)
    stop_words = array(string(), contains_null=False, nullable=False)


class FieldTerm(Schema):
    """One normalized metadata term with its field-local position."""

    document_id = string(nullable=False)
    field_name = string(nullable=False)
    term = string(nullable=False)
    position = long(nullable=False)
    analyzer_policy = string(nullable=False)
    phrase_enabled = boolean(nullable=False)


class FieldSearchQuery(Schema):
    """A field-aware query and its optional delegated full-text content."""

    id = string(nullable=False)
    queryset = string(nullable=False)
    query_text = string(nullable=False)
    content = string(nullable=False)
    requested_at = timestamp(nullable=False)
    labels = map(string(), long(), value_contains_null=False, nullable=False)
    is_question = boolean(nullable=False)
    is_time_sensitive = boolean(nullable=False)
    language = string(nullable=True)
    operator = string(nullable=False)
    clause_count = long(nullable=False)
    requires_content = boolean(nullable=False)


class FieldSearchDelegation(Schema):
    """Internal parent/child identity mapping for delegated body searches."""

    query_id = string(nullable=False)
    delegated_query_id = string(nullable=False)


class FieldSearchTerm(Schema):
    """One normalized metadata query term produced by field-query parsing."""

    query_id = string(nullable=False)
    clause_ordinal = long(nullable=False)
    term_ordinal = long(nullable=False)
    field_name = string(nullable=False)
    term = string(nullable=False)
    term_count = long(nullable=False)
    is_phrase = boolean(nullable=False)


class FieldSearchTermMatch(Schema):
    """Internal term match retaining the offset needed for phrase matching."""

    query_id = string(nullable=False)
    document_id = string(nullable=False)
    field_name = string(nullable=False)
    clause_ordinal = long(nullable=False)
    position_offset = long(nullable=False)
    matched_term_count = long(nullable=False)
    expected_term_count = long(nullable=False)


class FieldSearchClauseMatch(Schema):
    """Internal metadata clause match after phrase validation."""

    query_id = string(nullable=False)
    document_id = string(nullable=False)
    field_name = string(nullable=False)
    clause_ordinal = long(nullable=False)


class FieldSearchDocumentMatch(Schema):
    """Internal document match after boolean clause resolution."""

    query_id = string(nullable=False)
    document_id = string(nullable=False)
    matched_clause_count = long(nullable=False)
    expected_clause_count = long(nullable=False)


class FieldSearchResult(Schema):
    """A metadata-filtered or delegated field/content document result."""

    query_id = string(nullable=False)
    document_id = string(nullable=False)
    match_scope = string(nullable=False)
    document_result = struct(DocumentSearchResult, nullable=True)
