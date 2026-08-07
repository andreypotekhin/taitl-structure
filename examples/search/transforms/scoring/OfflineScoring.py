"""Bound offline query selection and reusable-index scoring."""

from examples.search.schemas.clicks import DailyImpressions
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentTerm,
    ParagraphIndexSummary,
    ParagraphTerm,
    SectionIndexSummary,
    SectionTerm,
    SentenceIndexSummary,
    SentenceTerm,
)
from examples.search.schemas.scoring.bm25 import (
    DocumentBm25Score,
    ParagraphBm25Score,
    SectionBm25Score,
    SentenceBm25Score,
)
from examples.search.schemas.scoring.overlap import (
    DocumentOverlapScore,
    ParagraphOverlapScore,
    SectionOverlapScore,
    SentenceOverlapScore,
)
from examples.search.schemas.search import (
    DocumentScore,
    ParagraphScore,
    ScorePolicy,
    SearchQuery,
    SectionScore,
    SentenceScore,
)
from examples.search.transforms.scoring.MergeOfflineQueries import MergeOfflineQueries
from examples.search.transforms.scoring.Scoring import Scoring
from examples.search.transforms.scoring.SelectPopularQueries import SelectPopularQueries
from examples.search.transforms.scoring.SelectRecentQueries import SelectRecentQueries
from structure import Transform, input, output, parameter


class OfflineScoring(Transform):
    """Select bounded offline queries and score them from reusable indexes."""

    queries = input(SearchQuery)
    daily_impressions = input(DailyImpressions)
    document_terms = input(DocumentTerm)
    section_terms = input(SectionTerm)
    paragraph_terms = input(ParagraphTerm)
    sentence_terms = input(SentenceTerm)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    score_policy = input(ScorePolicy)
    maximum_offline_queries = parameter(1000)

    popular = SelectPopularQueries(
        queries=queries,
        daily_impressions=daily_impressions,
        maximum_queries=maximum_offline_queries,
    )

    recent = SelectRecentQueries(
        queries=queries,
        daily_impressions=daily_impressions,
        score_policy=score_policy,
    )

    offline = MergeOfflineQueries(
        popular_queries=popular.selected_queries,
        recent_queries=recent.recent_queries,
    )

    scored = Scoring(
        queries=offline.offline_queries,
        document_terms=document_terms,
        section_terms=section_terms,
        paragraph_terms=paragraph_terms,
        sentence_terms=sentence_terms,
        document_summary=document_summary,
        section_summary=section_summary,
        paragraph_summary=paragraph_summary,
        sentence_summary=sentence_summary,
        score_policy=score_policy,
    )

    document_scores = output(DocumentScore, scored.document_scores)
    section_scores = output(SectionScore, scored.section_scores)
    paragraph_scores = output(ParagraphScore, scored.paragraph_scores)
    sentence_scores = output(SentenceScore, scored.sentence_scores)
    document_overlap_scores = output(DocumentOverlapScore, scored.document_overlap_scores)
    section_overlap_scores = output(SectionOverlapScore, scored.section_overlap_scores)
    paragraph_overlap_scores = output(ParagraphOverlapScore, scored.paragraph_overlap_scores)
    sentence_overlap_scores = output(SentenceOverlapScore, scored.sentence_overlap_scores)
    document_bm25_scores = output(DocumentBm25Score, scored.document_bm25_scores)
    section_bm25_scores = output(SectionBm25Score, scored.section_bm25_scores)
    paragraph_bm25_scores = output(ParagraphBm25Score, scored.paragraph_bm25_scores)
    sentence_bm25_scores = output(SentenceBm25Score, scored.sentence_bm25_scores)


__all__ = ["OfflineScoring"]
