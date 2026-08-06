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
from examples.search.schemas.search import DocumentScore, ParagraphScore, ScorePolicy, SectionScore, SentenceScore
from structure import Transform, input, output, parameter, step
from structure.plugin.pyspark import (
    cross_join,
    inner_join,
    rows_between,
    unbounded_following,
    unbounded_preceding,
    when,
    window,
    window_max,
)


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
    score_policy = input(ScorePolicy)
    experiment_id = parameter(None)

    @step(input=[document_overlap_scores, document_bm25_scores, score_policy], output=document_scores)
    def score_documents(
        self, overlap: DocumentOverlapScore, bm25: DocumentBm25Score, policy: ScorePolicy
    ) -> DocumentScore:
        inner_join(on=(bm25.document_id == overlap.document_id) & (bm25.query_id == overlap.query_id))
        cross_join(policy, allow_cartesian=True)
        maximum = window_max(
            bm25.score_bm25,
            over=window(
                partition_by=bm25.query_id,
                order_by=bm25.document_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        return DocumentScore.base(overlap)(
            experiment_id=self.experiment_id,
            scored_at=policy.scored_at,
            score=policy.document_bm25_weight * when(maximum > 0.0, bm25.score_bm25 / maximum).otherwise(0.0)
            + policy.document_overlap_weight * overlap.score_overlap,
        )

    @step(input=[section_overlap_scores, section_bm25_scores, score_policy], output=section_scores)
    def score_sections(self, overlap: SectionOverlapScore, bm25: SectionBm25Score, policy: ScorePolicy) -> SectionScore:
        inner_join(on=(bm25.section_id == overlap.section_id) & (bm25.query_id == overlap.query_id))
        cross_join(policy, allow_cartesian=True)
        maximum = window_max(
            bm25.score_bm25,
            over=window(
                partition_by=(bm25.query_id, bm25.document_id),
                order_by=bm25.section_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        return SectionScore.base(overlap)(
            experiment_id=self.experiment_id,
            scored_at=policy.scored_at,
            score=policy.section_bm25_weight * when(maximum > 0.0, bm25.score_bm25 / maximum).otherwise(0.0)
            + policy.section_overlap_weight * overlap.score_overlap,
        )

    @step(input=[paragraph_overlap_scores, paragraph_bm25_scores, score_policy], output=paragraph_scores)
    def score_paragraphs(
        self, overlap: ParagraphOverlapScore, bm25: ParagraphBm25Score, policy: ScorePolicy
    ) -> ParagraphScore:
        inner_join(on=(bm25.paragraph_id == overlap.paragraph_id) & (bm25.query_id == overlap.query_id))
        cross_join(policy, allow_cartesian=True)
        maximum = window_max(
            bm25.score_bm25,
            over=window(
                partition_by=(bm25.query_id, bm25.document_id, bm25.section_id),
                order_by=bm25.paragraph_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        return ParagraphScore.base(overlap)(
            experiment_id=self.experiment_id,
            scored_at=policy.scored_at,
            score=policy.paragraph_bm25_weight * when(maximum > 0.0, bm25.score_bm25 / maximum).otherwise(0.0)
            + policy.paragraph_overlap_weight * overlap.score_overlap,
        )

    @step(input=[sentence_overlap_scores, sentence_bm25_scores, score_policy], output=sentence_scores)
    def score_sentences(
        self, overlap: SentenceOverlapScore, bm25: SentenceBm25Score, policy: ScorePolicy
    ) -> SentenceScore:
        inner_join(on=(bm25.sentence_id == overlap.sentence_id) & (bm25.query_id == overlap.query_id))
        cross_join(policy, allow_cartesian=True)
        maximum = window_max(
            bm25.score_bm25,
            over=window(
                partition_by=(bm25.query_id, bm25.document_id, bm25.section_id, bm25.paragraph_id),
                order_by=bm25.sentence_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        return SentenceScore.base(overlap)(
            experiment_id=self.experiment_id,
            scored_at=policy.scored_at,
            score=policy.sentence_bm25_weight * when(maximum > 0.0, bm25.score_bm25 / maximum).otherwise(0.0)
            + policy.sentence_overlap_weight * overlap.score_overlap,
        )
