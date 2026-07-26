"""Public production-score selection boundary."""

from examples.search.schemas.search import (
    DocumentBm25Score,
    DocumentOverlapScore,
    DocumentScore,
    ParagraphBm25Score,
    ParagraphOverlapScore,
    ParagraphScore,
    SectionBm25Score,
    SectionOverlapScore,
    SectionScore,
    SentenceBm25Score,
    SentenceOverlapScore,
    SentenceScore,
)
from examples.search.transforms.scoring.ScoreAll import ScoreAll
from structure import lane, output, step
from structure.plugin.pyspark import inner_join


class AddScores(ScoreAll):
    """Run production scoring and select one unified score per target grain."""

    document_scores = output(DocumentScore)
    section_scores = output(SectionScore)
    paragraph_scores = output(ParagraphScore)
    sentence_scores = output(SentenceScore)

    @step(
        input=[lane(ScoreAll.document_overlap_scores), lane(ScoreAll.document_bm25_scores)],
        output=document_scores,
    )
    def score_documents(self, overlap: DocumentOverlapScore, bm25: DocumentBm25Score) -> DocumentScore:
        inner_join(on=(bm25.document_id == overlap.document_id) & (bm25.query_id == overlap.query_id))
        return DocumentScore.base(overlap)(experiment_id="", score=bm25.score_bm25)

    @step(
        input=[lane(ScoreAll.section_overlap_scores), lane(ScoreAll.section_bm25_scores)],
        output=section_scores,
    )
    def score_sections(self, overlap: SectionOverlapScore, bm25: SectionBm25Score) -> SectionScore:
        inner_join(on=(bm25.section_id == overlap.section_id) & (bm25.query_id == overlap.query_id))
        return SectionScore.base(overlap)(experiment_id="", score=bm25.score_bm25)

    @step(
        input=[lane(ScoreAll.paragraph_overlap_scores), lane(ScoreAll.paragraph_bm25_scores)],
        output=paragraph_scores,
    )
    def score_paragraphs(self, overlap: ParagraphOverlapScore, bm25: ParagraphBm25Score) -> ParagraphScore:
        inner_join(on=(bm25.paragraph_id == overlap.paragraph_id) & (bm25.query_id == overlap.query_id))
        return ParagraphScore.base(overlap)(experiment_id="", score=overlap.score_overlap)

    @step(
        input=[lane(ScoreAll.sentence_overlap_scores), lane(ScoreAll.sentence_bm25_scores)],
        output=sentence_scores,
    )
    def score_sentences(self, overlap: SentenceOverlapScore, bm25: SentenceBm25Score) -> SentenceScore:
        inner_join(on=(bm25.sentence_id == overlap.sentence_id) & (bm25.query_id == overlap.query_id))
        return SentenceScore.base(overlap)(experiment_id="", score=overlap.score_overlap)
