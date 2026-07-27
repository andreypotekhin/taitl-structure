"""Overlap scoring schemas."""

from examples.search.schemas.search import (
    DocumentSearchTarget,
    ParagraphSearchTarget,
    SectionSearchTarget,
    SentenceSearchTarget,
)
from structure import Schema
from structure.plugin.pyspark import double, long, string


class DocumentOverlapScore(DocumentSearchTarget):
    score_overlap = double(nullable=False)


class DocumentOverlapMatch(Schema):
    """Aggregate overlap numerator and denominator fields for one document."""

    query_id = string(nullable=False)
    document_id = string(nullable=False)
    query_terms = long(nullable=False)
    target_distinct_terms = long(nullable=False)
    matched_terms = long(nullable=False)


class SectionOverlapScore(SectionSearchTarget):
    score_overlap = double(nullable=False)


class SectionOverlapMatch(DocumentOverlapMatch):
    """Aggregate overlap fields for one section."""

    section_id = string(nullable=False)


class ParagraphOverlapScore(ParagraphSearchTarget):
    score_overlap = double(nullable=False)


class ParagraphOverlapMatch(SectionOverlapMatch):
    """Aggregate overlap fields for one paragraph."""

    paragraph_id = string(nullable=False)


class SentenceOverlapScore(SentenceSearchTarget):
    score_overlap = double(nullable=False)


class SentenceOverlapMatch(ParagraphOverlapMatch):
    """Aggregate overlap fields for one sentence."""

    sentence_id = string(nullable=False)
