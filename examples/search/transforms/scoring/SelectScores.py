"""Select production scores from reusable scoring families."""

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
from examples.search.schemas.search import DocumentScore, ParagraphScore, SectionScore, SentenceScore
from structure import Transform, input, output, step
from structure.plugin.pyspark import inner_join


class SelectScores(Transform):
    """Select one production score per scored search target."""

    document_overlap_scores = input(DocumentOverlapScore)
    section_overlap_scores = input(SectionOverlapScore)
    paragraph_overlap_scores = input(ParagraphOverlapScore)
    sentence_overlap_scores = input(SentenceOverlapScore)
    document_bm25_scores = input(DocumentBm25Score)
    section_bm25_scores = input(SectionBm25Score)
    paragraph_bm25_scores = input(ParagraphBm25Score)
    sentence_bm25_scores = input(SentenceBm25Score)
    document_scores = output(DocumentScore)
    section_scores = output(SectionScore)
    paragraph_scores = output(ParagraphScore)
    sentence_scores = output(SentenceScore)

    @step(input=[document_overlap_scores, document_bm25_scores], output=document_scores)
    def score_documents(self, overlap: DocumentOverlapScore, bm25: DocumentBm25Score) -> DocumentScore:
        inner_join(on=(bm25.document_id == overlap.document_id) & (bm25.query_id == overlap.query_id))
        return DocumentScore.base(overlap)(experiment_id="", score=bm25.score_bm25)

    @step(input=[section_overlap_scores, section_bm25_scores], output=section_scores)
    def score_sections(self, overlap: SectionOverlapScore, bm25: SectionBm25Score) -> SectionScore:
        inner_join(on=(bm25.section_id == overlap.section_id) & (bm25.query_id == overlap.query_id))
        return SectionScore.base(overlap)(experiment_id="", score=bm25.score_bm25)

    @step(input=[paragraph_overlap_scores, paragraph_bm25_scores], output=paragraph_scores)
    def score_paragraphs(self, overlap: ParagraphOverlapScore, bm25: ParagraphBm25Score) -> ParagraphScore:
        inner_join(on=(bm25.paragraph_id == overlap.paragraph_id) & (bm25.query_id == overlap.query_id))
        return ParagraphScore.base(overlap)(experiment_id="", score=overlap.score_overlap)

    @step(input=[sentence_overlap_scores, sentence_bm25_scores], output=sentence_scores)
    def score_sentences(self, overlap: SentenceOverlapScore, bm25: SentenceBm25Score) -> SentenceScore:
        inner_join(on=(bm25.sentence_id == overlap.sentence_id) & (bm25.query_id == overlap.query_id))
        return SentenceScore.base(overlap)(experiment_id="", score=overlap.score_overlap)
