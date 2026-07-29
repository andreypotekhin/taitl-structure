"""Scoring-internal intermediate schemas."""

from examples.search.schemas.search import (
    DocumentSearchTarget,
    ParagraphSearchTarget,
    SectionSearchTarget,
    SentenceSearchTarget,
)
from structure import Schema
from structure.plugin.pyspark import long, string


class QueryToken(Schema):
    """One normalized query token before row expansion."""

    token = string(nullable=False)


class ExpandedQueryToken(Schema):
    """One expanded query token with its original query-local ordinal."""

    ordinal = long(nullable=False)
    token = string(nullable=False)


class QueryTerm(Schema):
    """One distinct normalized query term."""

    query_id = string(nullable=False)
    token = string(nullable=False)


class QueryTermCount(Schema):
    """Number of distinct normalized terms in one query."""

    query_id = string(nullable=False)
    query_terms = long(nullable=False)


class DocumentOverlapMatch(DocumentSearchTarget):
    """Aggregate overlap numerator and denominator fields for one document."""

    query_terms = long(nullable=False)
    target_distinct_terms = long(nullable=False)
    matched_terms = long(nullable=False)


class SectionOverlapMatch(DocumentOverlapMatch, SectionSearchTarget):
    """Aggregate overlap fields for one section."""


class ParagraphOverlapMatch(SectionOverlapMatch, ParagraphSearchTarget):
    """Aggregate overlap fields for one paragraph."""


class SentenceOverlapMatch(ParagraphOverlapMatch, SentenceSearchTarget):
    """Aggregate overlap fields for one sentence."""
