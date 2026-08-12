"""Bound offline query selection and reusable-index scoring."""

from examples.search.schemas.clicks import *
from examples.search.schemas.indexing.lexical.index import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.scoring.bm25 import *
from examples.search.schemas.scoring.overlap import *
from examples.search.schemas.search import *
from examples.search.transforms.offline.scoring.lexical.MergeOfflineQueries import *
from examples.search.transforms.scoring import *
from examples.search.transforms.scoring.lexical.SelectPopularQueries import *
from examples.search.transforms.scoring.lexical.SelectRecentQueries import *
from structure import *


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
    document_vector_queries = input(DocumentVectorQuery, streaming=True)
    document_vector_index = input(DocumentVectorIndex)
    paragraph_vector_queries = input(ParagraphVectorQuery)
    paragraph_vector_index = input(ParagraphVectorIndex)
    vector_policy = input(VectorIndexPolicy)
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
        document_vector_queries=document_vector_queries,
        document_vector_index=document_vector_index,
        paragraph_vector_queries=paragraph_vector_queries,
        paragraph_vector_index=paragraph_vector_index,
        vector_policy=vector_policy,
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
    document_vector_scores = output(DocumentVectorScore, scored.document_vector_scores)
    paragraph_vector_scores = output(ParagraphVectorScore, scored.paragraph_vector_scores)
