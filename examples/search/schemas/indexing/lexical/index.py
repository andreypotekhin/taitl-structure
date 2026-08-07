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


class DocumentTerm(DocumentIndexTarget):
    term = string(nullable=False)
    term_frequency = long(nullable=False)
    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)
    target_frequency = long(nullable=False)


class SectionTerm(SectionIndexTarget):
    term = string(nullable=False)
    term_frequency = long(nullable=False)
    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)
    target_frequency = long(nullable=False)


class ParagraphTerm(ParagraphIndexTarget):
    term = string(nullable=False)
    term_frequency = long(nullable=False)
    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)
    target_frequency = long(nullable=False)


class SentenceTerm(SentenceIndexTarget):
    term = string(nullable=False)
    term_frequency = long(nullable=False)
    target_term_count = long(nullable=False)
    target_distinct_term_count = long(nullable=False)
    target_average_term_length = double(nullable=False)
    target_frequency = long(nullable=False)


class DocumentIndexSummary(Schema):
    target_count = long(nullable=False)
    average_target_length = double(nullable=False)


class SectionIndexSummary(DocumentIndexSummary):
    pass


class ParagraphIndexSummary(DocumentIndexSummary):
    pass


class SentenceIndexSummary(DocumentIndexSummary):
    pass
