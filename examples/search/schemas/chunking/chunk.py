"""Intermediate schemas for chunking searchable text rows."""

from structure import Schema
from structure.plugin.pyspark import array, boolean, double, integer, long, string


class DocumentLine(Schema):
    """Internal document line before ordinal expansion."""

    line = string(nullable=False)


class ExpandedDocumentLine(Schema):
    """Internal document line with its original document-local ordinal."""

    ordinal = long(nullable=False)
    line = string(nullable=False)


class MarkedDocumentLine(Schema):
    """Internal line annotated for section and paragraph grouping."""

    document_id = string(nullable=False)
    line_ordinal = long(nullable=False)
    line = string(nullable=False)
    heading = string(nullable=True)
    is_blank = boolean(nullable=False)
    section_ordinal = long(nullable=False)
    paragraph_group = long(nullable=False)


class ParagraphLine(Schema):
    """Internal body line belonging to one paragraph group."""

    document_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    paragraph_group = long(nullable=False)
    line_ordinal = long(nullable=False)
    line = string(nullable=False)


class SectionHeading(Schema):
    """Internal heading line for one section ordinal."""

    document_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    heading = string(nullable=False)


class ParagraphLineGroup(Schema):
    """Internal ordered paragraph lines before content assembly."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    paragraph_group = long(nullable=False)
    lines = array(string(), contains_null=False, nullable=False)


class ParagraphContent(Schema):
    """Internal paragraph content before per-section paragraph ranking."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    paragraph_group = long(nullable=False)
    content = string(nullable=False)


class ParagraphDraft(Schema):
    """Internal paragraph row retaining section grouping metadata."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    ordinal = integer(nullable=False)
    content = string(nullable=False)
    search_query_id = string(nullable=True)
    score_overlap = double(nullable=True)
    score_bm25 = double(nullable=True)


class SectionKey(Schema):
    """Internal distinct section key derived from paragraph rows."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    ordinal = integer(nullable=False)


class SentenceText(Schema):
    """Internal sentence text before ordinal expansion."""

    sentence_content = string(nullable=False)


class ExpandedSentenceText(Schema):
    """Internal sentence text with its paragraph-local ordinal."""

    position = long(nullable=False)
    sentence_content = string(nullable=False)


class WordText(Schema):
    """Internal word text before ordinal expansion."""

    word_token = string(nullable=False)


class ExpandedWordText(Schema):
    """Internal word text with its sentence-local ordinal."""

    position = long(nullable=False)
    word_token = string(nullable=False)
