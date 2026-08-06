"""Production scoring composition."""

from examples.search.adoption import SEARCH_STREAMING_CONTRACTS_ENABLED
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
from examples.search.transforms.scoring.ScoreBm25 import ScoreBm25
from examples.search.transforms.scoring.ScoreOverlap import ScoreOverlap
from examples.search.transforms.scoring.SelectScores import SelectScores
from structure import Transform, input, output, parameter


class Scoring(Transform):
    """Run production scoring and select one unified score per target grain."""

    queries = input(SearchQuery, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    score_policy = input(ScorePolicy)
    experiment_id = parameter(None)

    overlap = ScoreOverlap(
        queries=queries,
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

    bm25 = ScoreBm25(
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

    selected = SelectScores(
        document_overlap_scores=overlap.document_overlap_scores,
        section_overlap_scores=overlap.section_overlap_scores,
        paragraph_overlap_scores=overlap.paragraph_overlap_scores,
        sentence_overlap_scores=overlap.sentence_overlap_scores,
        document_bm25_scores=bm25.document_bm25_scores,
        section_bm25_scores=bm25.section_bm25_scores,
        paragraph_bm25_scores=bm25.paragraph_bm25_scores,
        sentence_bm25_scores=bm25.sentence_bm25_scores,
        score_policy=score_policy,
        experiment_id=experiment_id,
    )

    document_scores = output(DocumentScore, selected.document_scores)
    section_scores = output(SectionScore, selected.section_scores)
    paragraph_scores = output(ParagraphScore, selected.paragraph_scores)
    sentence_scores = output(SentenceScore, selected.sentence_scores)
    document_overlap_scores = output(DocumentOverlapScore, overlap.document_overlap_scores)
    section_overlap_scores = output(SectionOverlapScore, overlap.section_overlap_scores)
    paragraph_overlap_scores = output(ParagraphOverlapScore, overlap.paragraph_overlap_scores)
    sentence_overlap_scores = output(SentenceOverlapScore, overlap.sentence_overlap_scores)
    document_bm25_scores = output(DocumentBm25Score, bm25.document_bm25_scores)
    section_bm25_scores = output(SectionBm25Score, bm25.section_bm25_scores)
    paragraph_bm25_scores = output(ParagraphBm25Score, bm25.paragraph_bm25_scores)
    sentence_bm25_scores = output(SentenceBm25Score, bm25.sentence_bm25_scores)


__all__ = ["Scoring"]
