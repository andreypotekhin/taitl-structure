from structure import Schema
from structure.field import *


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


class Section(Schema):
    id = string(nullable=False)
    document_id = string(nullable=False)
    ordinal = integer(nullable=False)
    heading = string(nullable=False)


class Paragraph(Schema):
    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    ordinal = integer(nullable=False)
    content = string(nullable=False)


class Sentence(Schema):
    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    paragraph_ordinal = integer(nullable=False)
    ordinal = integer(nullable=False)
    content = string(nullable=False)


class Word(Schema):
    id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    paragraph_ordinal = integer(nullable=False)
    sentence_id = string(nullable=False)
    ordinal = integer(nullable=False)
    token = string(nullable=False)
