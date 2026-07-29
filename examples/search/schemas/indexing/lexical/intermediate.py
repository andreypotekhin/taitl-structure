"""Lexical indexing-internal schemas."""

from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexTarget,
    ParagraphIndexTarget,
    SectionIndexTarget,
    SentenceIndexTarget,
)
from structure import Schema
from structure.plugin.pyspark import long, string


class IndexTokenFrequency(Schema):
    """Internal count of indexed targets containing one token."""

    token = string(nullable=False)
    document_frequency = long(nullable=False)


class DocumentIndexTargetStats(DocumentIndexTarget):
    """Internal document-level token totals."""

    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)


class SectionIndexTargetStats(SectionIndexTarget):
    """Internal section-level token totals."""

    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)


class ParagraphIndexTargetStats(ParagraphIndexTarget):
    """Internal paragraph-level token totals."""

    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)


class SentenceIndexTargetStats(SentenceIndexTarget):
    """Internal sentence-level token totals."""

    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)


class DocumentIndexTermCount(DocumentIndexTarget):
    """Internal document-level term frequency."""

    token = string(nullable=False)
    term_frequency = long(nullable=False)


class SectionIndexTermCount(SectionIndexTarget):
    """Internal section-level term frequency."""

    token = string(nullable=False)
    term_frequency = long(nullable=False)


class ParagraphIndexTermCount(ParagraphIndexTarget):
    """Internal paragraph-level term frequency."""

    token = string(nullable=False)
    term_frequency = long(nullable=False)


class SentenceIndexTermCount(SentenceIndexTarget):
    """Internal sentence-level term frequency."""

    token = string(nullable=False)
    term_frequency = long(nullable=False)
