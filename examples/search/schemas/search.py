from structure import Schema
from structure.plugin.pyspark import *


class SearchQuery(Schema):
    """One caller-supplied full-text query."""

    id = string(nullable=False)
    queryset = string(nullable=False)
    content = string(nullable=False)
    requested_at = timestamp(nullable=False)
    labels = map(string(), long(), value_contains_null=False, nullable=False)
    is_question = boolean(nullable=False)
    is_time_sensitive = boolean(nullable=False)
    language = string(nullable=True)


class ScorePolicy(Schema):
    """Freshness, lexical-weight, and timestamp policy for score resolution."""

    maximum_age_days = long(nullable=False)
    scored_at = timestamp(nullable=False)
    effective_at = timestamp(nullable=False)
    document_bm25_weight = double(nullable=False)
    document_overlap_weight = double(nullable=False)
    section_bm25_weight = double(nullable=False)
    section_overlap_weight = double(nullable=False)
    paragraph_bm25_weight = double(nullable=False)
    paragraph_overlap_weight = double(nullable=False)
    sentence_bm25_weight = double(nullable=False)
    sentence_overlap_weight = double(nullable=False)


class QueryPopularity(Schema):
    """Aggregated offline popularity for one normalized query."""

    query = string(nullable=False)
    impression_count = long(nullable=False)


class SentenceSearchResult(Schema):
    """One ranked sentence match for a caller-supplied query."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=True)
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
    experiment_id = string(nullable=True)
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
    experiment_id = string(nullable=True)
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
    experiment_id = string(nullable=True)
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
    experiment_id = string(nullable=True)
    user_band_id = string(nullable=True)
    candidate_rank = long(nullable=False)
    document_id = string(nullable=False)
    query_feedback = double(nullable=True)


class PopularityFeedback(Schema):
    """Internal selected document-popularity feedback for one search candidate."""

    search_query_id = string(nullable=False)
    experiment_id = string(nullable=True)
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


class DocumentScore(DocumentSearchTarget):
    """One experiment-scoped unified document score."""

    experiment_id = string(nullable=True)
    scored_at = timestamp(nullable=False)
    score = double(nullable=False)


class SectionScore(SectionSearchTarget):
    """One experiment-scoped unified section score."""

    experiment_id = string(nullable=True)
    scored_at = timestamp(nullable=False)
    score = double(nullable=False)


class ParagraphScore(ParagraphSearchTarget):
    """One experiment-scoped unified paragraph score."""

    experiment_id = string(nullable=True)
    scored_at = timestamp(nullable=False)
    score = double(nullable=False)


class SentenceScore(SentenceSearchTarget):
    """One experiment-scoped unified sentence score."""

    experiment_id = string(nullable=True)
    scored_at = timestamp(nullable=False)
    score = double(nullable=False)
