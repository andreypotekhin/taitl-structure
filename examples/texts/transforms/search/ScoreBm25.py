"""BM25 search transform."""

from examples.texts.algorithms.ScoreBm25 import ScoreBm25 as Bm25Algorithm
from examples.texts.schemas.search import (
    DocumentBm25Score,
    ParagraphBm25Score,
    SearchQuery,
    SectionBm25Score,
    SentenceBm25Score,
)
from examples.texts.transforms.search.ScoreTargets import ScoreTargets
from structure import input, output, raw, step


class ScoreBm25(ScoreTargets):
    """Create BM25 scores for independent document-hierarchy targets."""

    document_bm25_scores = output(DocumentBm25Score)
    section_bm25_scores = output(SectionBm25Score)
    paragraph_bm25_scores = output(ParagraphBm25Score)
    sentence_bm25_scores = output(SentenceBm25Score)

    @step(
        input=ScoreTargets.queries,
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
                query_id=query.id,
                document_id="",
                section_id="",
                paragraph_id="",
                sentence_id="",
                score_bm25=0.0,
            ),
        )

    @raw(
        input=[input(ScoreTargets.queries), input(ScoreTargets.words)],
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
        words,
        document_bm25_scores,
        section_bm25_scores,
        paragraph_bm25_scores,
        sentence_bm25_scores,
        spark,
        ctx,
    ):
        return Bm25Algorithm.scores(
            queries,
            words,
            document_scores=document_bm25_scores,
            section_scores=section_bm25_scores,
            paragraph_scores=paragraph_bm25_scores,
            sentence_scores=sentence_bm25_scores,
        )
