from structure import Schema
from structure.plugin.pyspark import *


class SearchQuery(Schema):
    """One caller-supplied full-text query."""

    id = string(nullable=False)
    content = string(nullable=False)


class SentenceSearchResult(Schema):
    """One ranked sentence match for a caller-supplied query."""

    search_query_id = string(nullable=False)
    rank = long(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    sentence_id = string(nullable=False)
    content = string(nullable=False)
    score_overlap = double(nullable=False)
    score_bm25 = double(nullable=False)


class DocumentSearchTarget(Schema):
    query_id = string(nullable=False)
    document_id = string(nullable=False)


class SectionSearchTarget(DocumentSearchTarget):
    section_id = string(nullable=False)


class ParagraphSearchTarget(SectionSearchTarget):
    paragraph_id = string(nullable=False)


class SentenceSearchTarget(ParagraphSearchTarget):
    sentence_id = string(nullable=False)


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


class DocumentOverlapScore(DocumentSearchTarget):
    score_overlap = double(nullable=False)


class SectionOverlapScore(SectionSearchTarget):
    score_overlap = double(nullable=False)


class ParagraphOverlapScore(ParagraphSearchTarget):
    score_overlap = double(nullable=False)


class SentenceOverlapScore(SentenceSearchTarget):
    score_overlap = double(nullable=False)


class DocumentBm25Score(DocumentSearchTarget):
    score_bm25 = double(nullable=False)


class SectionBm25Score(SectionSearchTarget):
    score_bm25 = double(nullable=False)


class ParagraphBm25Score(ParagraphSearchTarget):
    score_bm25 = double(nullable=False)


class SentenceBm25Score(SentenceSearchTarget):
    score_bm25 = double(nullable=False)
