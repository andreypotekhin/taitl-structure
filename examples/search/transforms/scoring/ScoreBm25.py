"""BM25 scoring from reusable text-index artifacts."""

from examples.search.algorithms.scoring.ScoreBm25 import ScoreBm25 as Bm25Algorithm
from examples.search.schemas.search import (
    DocumentBm25Score,
    DocumentIndexSummary,
    ParagraphBm25Score,
    ParagraphIndexSummary,
    SearchQuery,
    SectionBm25Score,
    SectionIndexSummary,
    SentenceBm25Score,
    SentenceIndexSummary,
)
from examples.search.transforms.scoring.ScoreOverlap import ScoreOverlap
from structure import input, output, raw, step


class ScoreBm25(ScoreOverlap):
    """Score each query with BM25 over four reusable target indexes."""

    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    document_bm25_scores = output(DocumentBm25Score)
    section_bm25_scores = output(SectionBm25Score)
    paragraph_bm25_scores = output(ParagraphBm25Score)
    sentence_bm25_scores = output(SentenceBm25Score)

    @step(
        input=ScoreOverlap.queries,
        output=[document_bm25_scores, section_bm25_scores, paragraph_bm25_scores, sentence_bm25_scores],
    )
    def declare_bm25_scores(
        self, query: SearchQuery
    ) -> tuple[DocumentBm25Score, SectionBm25Score, ParagraphBm25Score, SentenceBm25Score]:
        return (
            DocumentBm25Score(query_id=query.id, document_id="", score_bm25=0.0),
            SectionBm25Score(query_id=query.id, document_id="", section_id="", score_bm25=0.0),
            ParagraphBm25Score(query_id=query.id, document_id="", section_id="", paragraph_id="", score_bm25=0.0),
            SentenceBm25Score(
                query_id=query.id, document_id="", section_id="", paragraph_id="", sentence_id="", score_bm25=0.0
            ),
        )

    @raw(
        input=[
            input(ScoreOverlap.queries),
            input(ScoreOverlap.document_terms),
            input(document_summary),
            input(ScoreOverlap.section_terms),
            input(section_summary),
            input(ScoreOverlap.paragraph_terms),
            input(paragraph_summary),
            input(ScoreOverlap.sentence_terms),
            input(sentence_summary),
        ],
        output=[
            output(document_bm25_scores),
            output(section_bm25_scores),
            output(paragraph_bm25_scores),
            output(sentence_bm25_scores),
        ],
    )
    def score_bm25(
        self,
        *,
        queries,
        document_terms,
        document_summary,
        section_terms,
        section_summary,
        paragraph_terms,
        paragraph_summary,
        sentence_terms,
        sentence_summary,
        document_bm25_scores,
        section_bm25_scores,
        paragraph_bm25_scores,
        sentence_bm25_scores,
        spark,
        ctx,
    ):
        return Bm25Algorithm.scores(
            queries,
            (document_terms, section_terms, paragraph_terms, sentence_terms),
            (document_summary, section_summary, paragraph_summary, sentence_summary),
            (document_bm25_scores, section_bm25_scores, paragraph_bm25_scores, sentence_bm25_scores),
        )
