from structure import Schema
from structure.plugin.pyspark import *


class Document(Schema):
    """Caller-extracted text and acquisition metadata for one document."""

    id = string(nullable=False)
    collection_id = string(nullable=False)
    source = string(nullable=False)
    title = string(nullable=False)
    url = string(nullable=True)
    content = string(nullable=False)
    content_type = string(nullable=False)
    encoding = string(nullable=False)
    language = string(nullable=False)
    created_at = timestamp(nullable=True)
    published_at = timestamp(nullable=True)
    harvested_at = timestamp(nullable=False)
    search_query_id = string(nullable=True)
    score_overlap = double(nullable=True)
    score_bm25 = double(nullable=True)

class Section(Schema):
    """Persisted document-local section span."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    ordinal = integer(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)
    heading_span_start = long(nullable=True)
    heading_span_end = long(nullable=True)


class Paragraph(Schema):
    """Persisted document-local paragraph span."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    ordinal = integer(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)


class Sentence(Schema):
    """Persisted document-local sentence span."""

    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    paragraph_ordinal = integer(nullable=False)
    ordinal = integer(nullable=False)
    span_start = long(nullable=False)
    span_end = long(nullable=False)
