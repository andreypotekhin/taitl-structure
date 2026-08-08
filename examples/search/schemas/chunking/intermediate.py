"""Intermediate schemas for chunking searchable text rows."""

from structure import Schema
from structure.plugin.pyspark import array, boolean, double, integer, long, string


class DocumentLine(Schema):
    """Internal canonical document line with source-local spans."""

    line = string(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)
    heading = string(nullable=True)
    heading_span_start = long(nullable=True)
    heading_span_end = long(nullable=True)
    is_blank = boolean(nullable=False)


class ExpandedDocumentLine(Schema):
    """Internal document line with ordinal and source-local spans."""

    ordinal = long(nullable=False)
    line = string(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)
    heading = string(nullable=True)
    heading_span_start = long(nullable=True)
    heading_span_end = long(nullable=True)
    is_blank = boolean(nullable=False)


class MarkedDocumentLine(Schema):
    """Internal line annotated for section and paragraph grouping."""

    document_id = string(nullable=False)
    line_ordinal = long(nullable=False)
    line = string(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)
    heading = string(nullable=True)
    heading_span_start = long(nullable=True)
    heading_span_end = long(nullable=True)
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
    span_start = long(nullable=False)
    span_end = long(nullable=False)


class SectionHeading(Schema):
    """Internal heading line for one section ordinal."""

    document_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    heading = string(nullable=False)
    heading_span_start = long(nullable=True)
    heading_span_end = long(nullable=True)


class ParagraphLineGroup(Schema):
    """Internal ordered paragraph lines before content assembly."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    paragraph_group = long(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)


class ParagraphContent(Schema):
    """Internal paragraph content before per-section paragraph ranking."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    paragraph_group = long(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)


class ParagraphDraft(Schema):
    """Internal paragraph row retaining section grouping metadata."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    ordinal = integer(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)


class SectionKey(Schema):
    """Internal distinct section key derived from paragraph rows."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_ordinal = long(nullable=False)
    ordinal = integer(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)


class SentenceText(Schema):
    """Internal sentence span before ordinal expansion."""

    local_start = long(nullable=False)
    local_end = long(nullable=False)
    sentence_content = string(nullable=False)

class ExpandedSentenceText(Schema):
    """Internal sentence text with its paragraph-local ordinal."""

    position = long(nullable=False)
    local_start = long(nullable=False)
    local_end = long(nullable=False)
    sentence_content = string(nullable=False)
