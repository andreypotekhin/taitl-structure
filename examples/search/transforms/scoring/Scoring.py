"""Production scoring composition."""

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
from examples.search.schemas.search import DocumentScore, ParagraphScore, SearchQuery, SectionScore, SentenceScore
from examples.search.transforms.scoring.ScoreBm25 import ScoreBm25
from examples.search.transforms.scoring.ScoreOverlap import ScoreOverlap
from examples.search.transforms.scoring.SelectScores import SelectScores
from structure import Transform, input, output, parameter, stage


class Scoring(Transform):
    """Run production scoring and select one unified score per target grain."""

    queries = input(SearchQuery)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    document_scores = output(DocumentScore)
    section_scores = output(SectionScore)
    paragraph_scores = output(ParagraphScore)
    sentence_scores = output(SentenceScore)
    experiment_id = parameter(None)

    overlap = stage(
        ScoreOverlap(
            queries=queries,
            document_terms=document_terms,
            section_terms=section_terms,
            paragraph_terms=paragraph_terms,
            sentence_terms=sentence_terms,
        )
    )
    bm25 = stage(
        ScoreBm25(
            queries=queries,
            document_terms=document_terms,
            section_terms=section_terms,
            paragraph_terms=paragraph_terms,
            sentence_terms=sentence_terms,
            document_summary=document_summary,
            section_summary=section_summary,
            paragraph_summary=paragraph_summary,
            sentence_summary=sentence_summary,
        )
    )
    selected = stage(
        SelectScores(
            document_overlap_scores=overlap.document_overlap_scores,
            section_overlap_scores=overlap.section_overlap_scores,
            paragraph_overlap_scores=overlap.paragraph_overlap_scores,
            sentence_overlap_scores=overlap.sentence_overlap_scores,
            document_bm25_scores=bm25.document_bm25_scores,
            section_bm25_scores=bm25.section_bm25_scores,
            paragraph_bm25_scores=bm25.paragraph_bm25_scores,
            sentence_bm25_scores=bm25.sentence_bm25_scores,
            experiment_id=experiment_id,
        )
    )


__all__ = ["Scoring"]
