"""Algorithm-specific search transforms for the texts example."""

from examples.texts.algorithms.score_bm25 import ScoreBm25
from examples.texts.algorithms.score_overlap import ScoreOverlap
from examples.texts.schemas.search import (
    DocumentBm25Score,
    DocumentOverlapScore,
    ParagraphBm25Score,
    ParagraphOverlapScore,
    SearchQuery,
    SectionBm25Score,
    SectionOverlapScore,
    SentenceBm25Score,
    SentenceOverlapScore,
)
from examples.texts.schemas.text import Word
from structure import *
from structure.plugin.pyspark import *


class SearchOverlap(Transform):
    """Create Overlap scores for independent document-hierarchy targets."""

    queries = input(SearchQuery)
    words = input(Word)
    document_scores = output(DocumentOverlapScore)
    section_scores = output(SectionOverlapScore)
    paragraph_scores = output(ParagraphOverlapScore)
    sentence_scores = output(SentenceOverlapScore)

    def declare_scores(
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
        input=[input(queries), input(words)],
        output=[output(document_scores), output(section_scores), output(paragraph_scores), output(sentence_scores)],
    )
    def score(
        self,
        *,
        queries,
        words,
        document_scores,
        section_scores,
        paragraph_scores,
        sentence_scores,
        spark,
        ctx,
    ):
        return ScoreOverlap.scores(
            queries,
            words,
            document_scores=document_scores,
            section_scores=section_scores,
            paragraph_scores=paragraph_scores,
            sentence_scores=sentence_scores,
        )


class SearchBm25(Transform):
    """Create BM25 scores for independent document-hierarchy targets."""

    queries = input(SearchQuery)
    words = input(Word)
    document_scores = output(DocumentBm25Score)
    section_scores = output(SectionBm25Score)
    paragraph_scores = output(ParagraphBm25Score)
    sentence_scores = output(SentenceBm25Score)

    def declare_scores(
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
        input=[input(queries), input(words)],
        output=[output(document_scores), output(section_scores), output(paragraph_scores), output(sentence_scores)],
    )
    def score(
        self,
        *,
        queries,
        words,
        document_scores,
        section_scores,
        paragraph_scores,
        sentence_scores,
        spark,
        ctx,
    ):
        return ScoreBm25.scores(
            queries,
            words,
            document_scores=document_scores,
            section_scores=section_scores,
            paragraph_scores=paragraph_scores,
            sentence_scores=sentence_scores,
        )
