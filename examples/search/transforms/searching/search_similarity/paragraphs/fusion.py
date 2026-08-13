"""Fuse adopted lexical and vector paragraph candidates."""

from examples.search.schemas.similarities.vector import ParagraphFusedSimilarityCandidate
from examples.search.schemas.similarity import SimilarityFusionPolicy
from examples.search.transforms.lib.Rrf import Rrf
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import group_by, max, param_join, require_all, union_all, where


class FuseSimilarity(Transform):
    """Deduplicate paragraph retrieval lanes and calculate their RRF score."""

    paragraph_lexical_candidates = input(ParagraphFusedSimilarityCandidate)
    paragraph_vector_candidates = input(ParagraphFusedSimilarityCandidate)
    policy = input(SimilarityFusionPolicy)
    valid_policy = lane(SimilarityFusionPolicy)
    merged_candidates = lane(ParagraphFusedSimilarityCandidate)
    fused_candidates = lane(ParagraphFusedSimilarityCandidate)
    scored_candidates = lane(ParagraphFusedSimilarityCandidate)
    paragraph_candidates = output(ParagraphFusedSimilarityCandidate)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: SimilarityFusionPolicy) -> SimilarityFusionPolicy:
        validated = require_all(
            (policy.rrf_k > 0)
            & (policy.maximum_lexical_candidates > 0)
            & (policy.maximum_vector_candidates > 0)
            & (policy.maximum_results > 0)
        )
        return SimilarityFusionPolicy.project(validated)

    @step(input=[paragraph_lexical_candidates, paragraph_vector_candidates], output=merged_candidates)
    def merge_paragraphs(
        self, lexical: ParagraphFusedSimilarityCandidate, vector: ParagraphFusedSimilarityCandidate
    ) -> ParagraphFusedSimilarityCandidate:
        merged = union_all(vector)
        return ParagraphFusedSimilarityCandidate.project(merged)

    @step(input=[merged_candidates, valid_policy], output=fused_candidates)
    def fuse_paragraphs(
        self, candidate: ParagraphFusedSimilarityCandidate, policy: SimilarityFusionPolicy
    ) -> ParagraphFusedSimilarityCandidate:
        param_join(policy)
        group_by(
            left_document_id=candidate.left_document_id,
            left_section_id=candidate.left_section_id,
            left_paragraph_id=candidate.left_paragraph_id,
            right_document_id=candidate.right_document_id,
            right_section_id=candidate.right_section_id,
            right_paragraph_id=candidate.right_paragraph_id,
            rrf_k=policy.rrf_k,
            experiment_id=policy.experiment_id,
        )
        return ParagraphFusedSimilarityCandidate(
            left_document_id=candidate.left_document_id,
            left_section_id=candidate.left_section_id,
            left_paragraph_id=candidate.left_paragraph_id,
            right_document_id=candidate.right_document_id,
            right_section_id=candidate.right_section_id,
            right_paragraph_id=candidate.right_paragraph_id,
            lexical_rank=max(candidate.lexical_rank),
            vector_rank=max(candidate.vector_rank),
            score_overlap=max(candidate.score_overlap),
            bm25_left_to_right=max(candidate.bm25_left_to_right),
            bm25_right_to_left=max(candidate.bm25_right_to_left),
            bm25_mean=max(candidate.bm25_mean),
            vector_similarity=max(candidate.vector_similarity),
            vector_backend=max(candidate.vector_backend),
            vector_model_id=max(candidate.vector_model_id),
            vector_dimension=max(candidate.vector_dimension),
            vector_content_revision=max(candidate.vector_content_revision),
            rrf_score=max(candidate.rrf_score),
            rrf_k=policy.rrf_k,
            experiment_id=policy.experiment_id,
        )

    @step(input=[fused_candidates, valid_policy], output=scored_candidates)
    def score_paragraphs(
        self, candidate: ParagraphFusedSimilarityCandidate, policy: SimilarityFusionPolicy
    ) -> ParagraphFusedSimilarityCandidate:
        param_join(policy)
        return ParagraphFusedSimilarityCandidate.project(candidate)(
            rrf_score=Rrf.score(candidate.lexical_rank, candidate.vector_rank, policy.rrf_k)
        )

    @step(input=[scored_candidates, valid_policy], output=paragraph_candidates)
    def publish_paragraphs(
        self, candidate: ParagraphFusedSimilarityCandidate, policy: SimilarityFusionPolicy
    ) -> ParagraphFusedSimilarityCandidate:
        param_join(policy)
        where(candidate.lexical_rank.is_null() | (candidate.lexical_rank <= policy.maximum_lexical_candidates))
        where(candidate.vector_rank.is_null() | (candidate.vector_rank <= policy.maximum_vector_candidates))
        return ParagraphFusedSimilarityCandidate.project(candidate)
