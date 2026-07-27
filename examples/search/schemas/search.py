from structure import Schema
from structure.plugin.pyspark import *


class SearchQuery(Schema):
    """One caller-supplied full-text query."""

    id = string(nullable=False)
    content = string(nullable=False)
    labels = map(string(), long(), value_contains_null=False, nullable=False)
    is_question = boolean(nullable=False)
    is_time_sensitive = boolean(nullable=False)


class QueryToken(Schema):
    """One normalized query token before row expansion."""

    token = string(nullable=False)


class ExpandedQueryToken(Schema):
    """One expanded query token with its original query-local ordinal."""

    ordinal = long(nullable=False)
    token = string(nullable=False)


class QueryTerm(Schema):
    """One distinct normalized query term."""

    query_id = string(nullable=False)
    token = string(nullable=False)


class QueryTermCount(Schema):
    """Number of distinct normalized terms in one query."""

    query_id = string(nullable=False)
    query_terms = long(nullable=False)


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
    """A document search result."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    user_band_id = string(nullable=True)
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
    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    user_band_id = string(nullable=True)
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


class DocumentFeedbackOption(DocumentSearchCandidate):
    """Internal candidate row bound to one feedback fallback context."""

    feedback_band_id = string(nullable=True)
    fallback_ordinal = long(nullable=False)
    minimum_band_impressions = long(nullable=False)


class QueryDocumentFeedback(Schema):
    """Internal selected query/document feedback for one search candidate."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    user_band_id = string(nullable=True)
    candidate_rank = long(nullable=False)
    document_id = string(nullable=False)
    query_feedback = double(nullable=True)


class PopularityFeedback(Schema):
    """Internal selected document-popularity feedback for one search candidate."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=False)
    user_band_id = string(nullable=True)
    candidate_rank = long(nullable=False)
    document_id = string(nullable=False)
    popularity_feedback = double(nullable=True)


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
