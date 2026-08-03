"""Fill missing or stale document-search scores from reusable indexes."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentIndexTerm,
    ParagraphIndexSummary,
    ParagraphIndexTerm,
    SectionIndexSummary,
    SectionIndexTerm,
    SentenceIndexSummary,
    SentenceIndexTerm,
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
from examples.search.transforms.scoring.Scoring import Scoring
from examples.search.transforms.searching.online.scoring.SelectGapQueries import SelectGapQueries
from structure import Transform, input, output, stage


class OnlineScoring(Transform):
    """Calculate only score groups missing or stale in caller-supplied scores."""

    queries = input(SearchQuery)
    requests = input(SearchRequest)
    document_scores = input(DocumentScore)
    document_overlap_scores = input(DocumentOverlapScore)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    score_policy = input(ScorePolicy)

    gap = stage(
        SelectGapQueries(
            queries=queries,
            requests=requests,
            document_scores=document_scores,
            document_overlap_scores=document_overlap_scores,
            score_policy=score_policy,
        )
    )

    scoring = stage(
        Scoring(
            queries=gap.gap_queries,
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
    )

    online_document_scores = output(DocumentScore, scoring.document_scores)
    online_section_scores = output(SectionScore, scoring.section_scores)
    online_paragraph_scores = output(ParagraphScore, scoring.paragraph_scores)
    online_sentence_scores = output(SentenceScore, scoring.sentence_scores)
    online_document_overlap_scores = output(DocumentOverlapScore, scoring.document_overlap_scores)
    online_section_overlap_scores = output(SectionOverlapScore, scoring.section_overlap_scores)
    online_paragraph_overlap_scores = output(ParagraphOverlapScore, scoring.paragraph_overlap_scores)
    online_sentence_overlap_scores = output(SentenceOverlapScore, scoring.sentence_overlap_scores)


__all__ = ["OnlineScoring"]
