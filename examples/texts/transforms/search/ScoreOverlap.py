"""Overlap-coefficient search transform."""

from examples.texts.algorithms.ScoreOverlap import ScoreOverlap as OverlapAlgorithm
from examples.texts.schemas.search import (
    DocumentOverlapScore,
    ParagraphOverlapScore,
    SearchQuery,
    SectionOverlapScore,
    SentenceOverlapScore,
)
from examples.texts.transforms.search.ScoreTargets import ScoreTargets
from structure import input, output, raw, step


class ScoreOverlap(ScoreTargets):
    """Create overlap scores for independent document-hierarchy targets."""

    document_overlap_scores = output(DocumentOverlapScore)
    section_overlap_scores = output(SectionOverlapScore)
    paragraph_overlap_scores = output(ParagraphOverlapScore)
    sentence_overlap_scores = output(SentenceOverlapScore)

    @step(
        input=ScoreTargets.queries,
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
                query_id=query.id,
                document_id="",
                section_id="",
                paragraph_id="",
                sentence_id="",
                score_overlap=0.0,
            ),
        )

    @raw(
        input=[input(ScoreTargets.queries), input(ScoreTargets.words)],
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
        words,
        document_overlap_scores,
        section_overlap_scores,
        paragraph_overlap_scores,
        sentence_overlap_scores,
        spark,
        ctx,
    ):
        return OverlapAlgorithm.scores(
            queries,
            words,
            document_scores=document_overlap_scores,
            section_scores=section_overlap_scores,
            paragraph_scores=paragraph_overlap_scores,
            sentence_scores=sentence_overlap_scores,
        )
