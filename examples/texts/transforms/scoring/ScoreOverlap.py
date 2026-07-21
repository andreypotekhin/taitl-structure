"""Overlap scoring from reusable text-index artifacts."""

from examples.texts.algorithms.scoring.ScoreOverlap import ScoreOverlap as OverlapAlgorithm
from examples.texts.schemas.search import (
    DocumentIndexTerm,
    DocumentOverlapScore,
    ParagraphIndexTerm,
    ParagraphOverlapScore,
    SearchQuery,
    SectionIndexTerm,
    SectionOverlapScore,
    SentenceIndexTerm,
    SentenceOverlapScore,
)
from structure import Transform, input, output, raw, step


class ScoreOverlap(Transform):
    """Score each query against reusable indexes at four target grains."""

    queries = input(SearchQuery)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
    document_overlap_scores = output(DocumentOverlapScore)
    section_overlap_scores = output(SectionOverlapScore)
    paragraph_overlap_scores = output(ParagraphOverlapScore)
    sentence_overlap_scores = output(SentenceOverlapScore)

    @step(
        input=queries,
        output=[document_overlap_scores, section_overlap_scores, paragraph_overlap_scores, sentence_overlap_scores],
    )
    def declare_overlap_scores(
        self, query: SearchQuery
    ) -> tuple[DocumentOverlapScore, SectionOverlapScore, ParagraphOverlapScore, SentenceOverlapScore]:
        return (
            DocumentOverlapScore(query_id=query.id, document_id="", score_overlap=0.0),
            SectionOverlapScore(query_id=query.id, document_id="", section_id="", score_overlap=0.0),
            ParagraphOverlapScore(query_id=query.id, document_id="", section_id="", paragraph_id="", score_overlap=0.0),
            SentenceOverlapScore(
                query_id=query.id, document_id="", section_id="", paragraph_id="", sentence_id="", score_overlap=0.0
            ),
        )

    @raw(
        input=[
            input(queries),
            input(document_terms),
            input(section_terms),
            input(paragraph_terms),
            input(sentence_terms),
        ],
        output=[
            output(document_overlap_scores),
            output(section_overlap_scores),
            output(paragraph_overlap_scores),
            output(sentence_overlap_scores),
        ],
    )
    def score_overlap(
        self,
        *,
        queries,
        document_terms,
        section_terms,
        paragraph_terms,
        sentence_terms,
        document_overlap_scores,
        section_overlap_scores,
        paragraph_overlap_scores,
        sentence_overlap_scores,
        spark,
        ctx,
    ):
        return OverlapAlgorithm.scores(
            queries,
            (document_terms, section_terms, paragraph_terms, sentence_terms),
            (document_overlap_scores, section_overlap_scores, paragraph_overlap_scores, sentence_overlap_scores),
        )
