"""Reduce directed index scores into reciprocal canonical similarity pairs."""

from examples.texts.algorithms.similarity.SimilarityScores import SimilarityScores
from examples.texts.schemas.search import (
    DocumentBm25Score,
    DocumentOverlapScore,
    ParagraphBm25Score,
    ParagraphOverlapScore,
    SectionBm25Score,
    SectionOverlapScore,
    SentenceBm25Score,
    SentenceOverlapScore,
)
from examples.texts.schemas.similarity import (
    DocumentSimilarity,
    DocumentSimilarityQuery,
    ParagraphSimilarity,
    ParagraphSimilarityQuery,
    SectionSimilarity,
    SectionSimilarityQuery,
    SentenceSimilarity,
    SentenceSimilarityQuery,
)
from structure import Transform, input, output, raw, step


class ReduceSimilarityScores(Transform):
    """Emit same-grain candidates with overlap and reciprocal BM25 evidence."""

    document_queries = input(DocumentSimilarityQuery)
    section_queries = input(SectionSimilarityQuery)
    paragraph_queries = input(ParagraphSimilarityQuery)
    sentence_queries = input(SentenceSimilarityQuery)
    document_overlap_scores = input(DocumentOverlapScore)
    section_overlap_scores = input(SectionOverlapScore)
    paragraph_overlap_scores = input(ParagraphOverlapScore)
    sentence_overlap_scores = input(SentenceOverlapScore)
    document_bm25_scores = input(DocumentBm25Score)
    section_bm25_scores = input(SectionBm25Score)
    paragraph_bm25_scores = input(ParagraphBm25Score)
    sentence_bm25_scores = input(SentenceBm25Score)
    document_similarities = output(DocumentSimilarity)
    section_similarities = output(SectionSimilarity)
    paragraph_similarities = output(ParagraphSimilarity)
    sentence_similarities = output(SentenceSimilarity)

    @step(
        input=document_queries,
        output=[document_similarities, section_similarities, paragraph_similarities, sentence_similarities],
    )
    def declare_similarities(
        self, query: DocumentSimilarityQuery
    ) -> tuple[DocumentSimilarity, SectionSimilarity, ParagraphSimilarity, SentenceSimilarity]:
        return (
            DocumentSimilarity(
                left_document_id=query.document_id,
                right_document_id="",
                score_overlap=0.0,
                bm25_left_to_right=0.0,
                bm25_right_to_left=0.0,
                bm25_mean=0.0,
            ),
            SectionSimilarity(
                left_document_id=query.document_id,
                left_section_id="",
                right_document_id="",
                right_section_id="",
                score_overlap=0.0,
                bm25_left_to_right=0.0,
                bm25_right_to_left=0.0,
                bm25_mean=0.0,
            ),
            ParagraphSimilarity(
                left_document_id=query.document_id,
                left_section_id="",
                left_paragraph_id="",
                right_document_id="",
                right_section_id="",
                right_paragraph_id="",
                score_overlap=0.0,
                bm25_left_to_right=0.0,
                bm25_right_to_left=0.0,
                bm25_mean=0.0,
            ),
            SentenceSimilarity(
                left_document_id=query.document_id,
                left_section_id="",
                left_paragraph_id="",
                left_sentence_id="",
                right_document_id="",
                right_section_id="",
                right_paragraph_id="",
                right_sentence_id="",
                score_overlap=0.0,
                bm25_left_to_right=0.0,
                bm25_right_to_left=0.0,
                bm25_mean=0.0,
            ),
        )

    @raw(
        input=[
            input(document_queries),
            input(section_queries),
            input(paragraph_queries),
            input(sentence_queries),
            input(document_overlap_scores),
            input(section_overlap_scores),
            input(paragraph_overlap_scores),
            input(sentence_overlap_scores),
            input(document_bm25_scores),
            input(section_bm25_scores),
            input(paragraph_bm25_scores),
            input(sentence_bm25_scores),
        ],
        output=[
            output(document_similarities),
            output(section_similarities),
            output(paragraph_similarities),
            output(sentence_similarities),
        ],
    )
    def reduce(
        self,
        *,
        document_queries,
        section_queries,
        paragraph_queries,
        sentence_queries,
        document_overlap_scores,
        section_overlap_scores,
        paragraph_overlap_scores,
        sentence_overlap_scores,
        document_bm25_scores,
        section_bm25_scores,
        paragraph_bm25_scores,
        sentence_bm25_scores,
        document_similarities,
        section_similarities,
        paragraph_similarities,
        sentence_similarities,
        spark,
        ctx,
    ):
        return SimilarityScores.reduce(
            (document_queries, section_queries, paragraph_queries, sentence_queries),
            (document_overlap_scores, section_overlap_scores, paragraph_overlap_scores, sentence_overlap_scores),
            (document_bm25_scores, section_bm25_scores, paragraph_bm25_scores, sentence_bm25_scores),
            (document_similarities, section_similarities, paragraph_similarities, sentence_similarities),
        )
