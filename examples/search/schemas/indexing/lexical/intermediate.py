"""Lexical indexing-internal schemas."""

from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexTarget,
    ParagraphIndexTarget,
    SectionIndexTarget,
    SentenceIndexTarget,
)
from structure import Schema
from structure.plugin.pyspark import double, long, string


class TermText(Schema):
    """Internal sentence term before expansion."""

    term = string(nullable=False)


class ExpandedTermText(Schema):
    """Internal sentence term with its source-local position."""

    position = long(nullable=False)
    term = string(nullable=False)


class LexicalOccurrence(Schema):
    """Private transient normalized occurrence used only while building aggregates."""

    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    sentence_id = string(nullable=False)
    term = string(nullable=False)


class DocumentHierarchyCounts(DocumentIndexTarget):
    """Private document hierarchy counts derived from sentence terms."""

    section_count = long(nullable=False)
    paragraph_count = long(nullable=False)
    sentence_count = long(nullable=False)


class IndexTargetFrequency(Schema):
    """Internal count of indexed targets containing one term."""

    term = string(nullable=False)
    target_frequency = long(nullable=False)


class DocumentIndexTargetStats(DocumentIndexTarget):
    """Internal document-level term totals."""

    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)


class SectionIndexTargetStats(SectionIndexTarget):
    """Internal section-level term totals."""

    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)


class ParagraphIndexTargetStats(ParagraphIndexTarget):
    """Internal paragraph-level term totals."""

    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)


class SentenceIndexTargetStats(SentenceIndexTarget):
    """Internal sentence-level term totals."""

    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)


class DocumentTermCount(DocumentIndexTarget):
    """Internal document-level term frequency."""

    term = string(nullable=False)
    term_frequency = long(nullable=False)


class SectionTermCount(SectionIndexTarget):
    """Internal section-level term frequency."""

    term = string(nullable=False)
    term_frequency = long(nullable=False)


class ParagraphTermCount(ParagraphIndexTarget):
    """Internal paragraph-level term frequency."""

    term = string(nullable=False)
    term_frequency = long(nullable=False)


class SentenceTermCount(SentenceIndexTarget):
    """Internal sentence-level term frequency."""

    term = string(nullable=False)
    term_frequency = long(nullable=False)
