"""Overlap scoring from reusable text-index artifacts."""

from examples.search.algorithms.scoring.ScoreOverlap import ScoreOverlap as OverlapAlgorithm
from examples.search.schemas.search import (
    DocumentOverlapScore,
    ParagraphOverlapScore,
    SearchQuery,
    SectionOverlapScore,
    SentenceOverlapScore,
)
from examples.search.transforms.scoring.ScoreBase import ScoreBase
from structure import input, output, raw, step


class ScoreOverlap(ScoreBase):
    """Score each query against reusable indexes at four target grains."""

    document_overlap_scores = output(DocumentOverlapScore)
    section_overlap_scores = output(SectionOverlapScore)
    paragraph_overlap_scores = output(ParagraphOverlapScore)
    sentence_overlap_scores = output(SentenceOverlapScore)

    @step(
        input=ScoreBase.queries,
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
            input(ScoreBase.queries),
            input(ScoreBase.document_terms),
            input(ScoreBase.section_terms),
            input(ScoreBase.paragraph_terms),
            input(ScoreBase.sentence_terms),
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
