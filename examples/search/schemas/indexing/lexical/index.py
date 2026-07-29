"""Public lexical index schemas."""

from structure import Schema
from structure.plugin.pyspark import double, long, string


class DocumentIndexTarget(Schema):
    document_id = string(nullable=False)


class SectionIndexTarget(DocumentIndexTarget):
    section_id = string(nullable=False)


class ParagraphIndexTarget(SectionIndexTarget):
    paragraph_id = string(nullable=False)


class SentenceIndexTarget(ParagraphIndexTarget):
    sentence_id = string(nullable=False)


class DocumentIndexTerm(DocumentIndexTarget):
    token = string(nullable=False)
    term_frequency = long(nullable=False)
    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)
    document_frequency = long(nullable=False)


class SectionIndexTerm(SectionIndexTarget):
    token = string(nullable=False)
    term_frequency = long(nullable=False)
    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)
    document_frequency = long(nullable=False)


class ParagraphIndexTerm(ParagraphIndexTarget):
    token = string(nullable=False)
    term_frequency = long(nullable=False)
    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)
    document_frequency = long(nullable=False)


class SentenceIndexTerm(SentenceIndexTarget):
    token = string(nullable=False)
    term_frequency = long(nullable=False)
    target_word_count = long(nullable=False)
    target_distinct_terms = long(nullable=False)
    document_frequency = long(nullable=False)


class DocumentIndexSummary(Schema):
    target_count = long(nullable=False)
    average_target_length = double(nullable=False)


class SectionIndexSummary(DocumentIndexSummary):
    pass


class ParagraphIndexSummary(DocumentIndexSummary):
    pass


class SentenceIndexSummary(DocumentIndexSummary):
    pass
