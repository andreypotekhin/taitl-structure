from structure import Schema
from structure.plugin.pyspark import *


class SearchQuery(Schema):
    """One caller-supplied full-text query."""

    id = string(nullable=False)
    content = string(nullable=False)
    labels = map(string(), long(), value_contains_null=False, nullable=False)
    is_question = boolean(nullable=False)
    is_time_sensitive = boolean(nullable=False)


class SentenceSearchResult(Schema):
    """One ranked sentence match for a caller-supplied query."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    rank = long(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    sentence_id = string(nullable=False)
    content = string(nullable=False)
    score = double(nullable=False)


class PassageSearchResult(Schema):
    """One ranked paragraph match with same-section answer context."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    rank = long(nullable=False)
    document_id = string(nullable=False)
    title = string(nullable=False)
    url = string(nullable=True)
    section_id = string(nullable=False)
    section_heading = string(nullable=False)
    paragraph_id = string(nullable=False)
    preceding_content = string(nullable=True)
    content = string(nullable=False)
    following_content = string(nullable=True)
    score = double(nullable=False)


class ParagraphContext(Schema):
    """One paragraph and its immediate same-section neighbors."""

    paragraph_id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    content = string(nullable=False)
    preceding_content = string(nullable=True)
    following_content = string(nullable=True)


class DocumentSearchResult(Schema):
    """One two-stage ranked document result."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    band_id = string(nullable=True)
    rank = long(nullable=False)
    candidate_rank = long(nullable=False)
    document_id = string(nullable=False)
    title = string(nullable=False)
    url = string(nullable=True)
    score = double(nullable=False)
    score_feedback = double(nullable=False)
    score_rank = double(nullable=False)


class DocumentSearchCandidate(Schema):
    """One document candidate while two-stage search ranks it."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    band_id = string(nullable=True)
    query = string(nullable=False)
    candidate_rank = long(nullable=False)
    document_id = string(nullable=False)
    title = string(nullable=False)
    url = string(nullable=True)
    score = double(nullable=False)
    score_feedback = double(nullable=False)
    score_rank = double(nullable=False)
    score_weight = double(nullable=False)
    feedback_weight = double(nullable=False)


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


class DocumentScore(DocumentSearchTarget):
    """One experiment-scoped unified document score."""

    experiment_id = string(nullable=False)
    score = double(nullable=False)


class SectionScore(SectionSearchTarget):
    """One experiment-scoped unified section score."""

    experiment_id = string(nullable=False)
    score = double(nullable=False)


class ParagraphScore(ParagraphSearchTarget):
    """One experiment-scoped unified paragraph score."""

    experiment_id = string(nullable=False)
    score = double(nullable=False)


class SentenceScore(SentenceSearchTarget):
    """One experiment-scoped unified sentence score."""

    experiment_id = string(nullable=False)
    score = double(nullable=False)
