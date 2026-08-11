"""Fuse adopted lexical and vector similarity candidates."""

from examples.search.schemas.indexing.vector import VectorIndexPolicy
from examples.search.schemas.similarities.vector import (
    DocumentFusedSimilarityCandidate,
    ParagraphFusedSimilarityCandidate,
)
from examples.search.transforms.lib.Rrf import Rrf
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import group_by, max, param_join, union_all


class FuseSimilarity(Transform):
    """Deduplicate same-grain retrieval lanes and calculate their RRF score."""

    policy = input(VectorIndexPolicy)
    document_lexical_candidates = input(DocumentFusedSimilarityCandidate)
    document_vector_candidates = input(DocumentFusedSimilarityCandidate)
    merged_document_candidates = lane(DocumentFusedSimilarityCandidate)
    fused_document_candidates = lane(DocumentFusedSimilarityCandidate)
    scored_document_candidates = lane(DocumentFusedSimilarityCandidate)
    document_candidates = output(DocumentFusedSimilarityCandidate)

    @step(input=[document_lexical_candidates, document_vector_candidates], output=merged_document_candidates)
    def merge_documents(
        self, lexical: DocumentFusedSimilarityCandidate, vector: DocumentFusedSimilarityCandidate
    ) -> DocumentFusedSimilarityCandidate:
        merged = union_all(vector)
        return DocumentFusedSimilarityCandidate.project(merged)

    @step(input=[merged_document_candidates, policy], output=fused_document_candidates)
    def fuse_documents(
        self, candidate: DocumentFusedSimilarityCandidate, policy: VectorIndexPolicy
    ) -> DocumentFusedSimilarityCandidate:
        param_join(policy)
        lexical_rank = max(candidate.lexical_rank)
        vector_rank = max(candidate.vector_rank)
        group_by(
            left_document_id=candidate.left_document_id,
            right_document_id=candidate.right_document_id,
            rrf_k=policy.rrf_k,
            experiment_id=policy.experiment_id,
        )
        return DocumentFusedSimilarityCandidate(
            left_document_id=candidate.left_document_id,
            right_document_id=candidate.right_document_id,
            lexical_rank=lexical_rank,
            vector_rank=vector_rank,
            score_overlap=max(candidate.score_overlap),
            bm25_left_to_right=max(candidate.bm25_left_to_right),
            bm25_right_to_left=max(candidate.bm25_right_to_left),
            bm25_mean=max(candidate.bm25_mean),
            vector_similarity=max(candidate.vector_similarity),
            rrf_score=max(candidate.rrf_score),
            rrf_k=policy.rrf_k,
            experiment_id=policy.experiment_id,
        )

    @step(input=[fused_document_candidates, policy], output=scored_document_candidates)
    def score_documents(
        self, candidate: DocumentFusedSimilarityCandidate, policy: VectorIndexPolicy
    ) -> DocumentFusedSimilarityCandidate:
        param_join(policy)
        return DocumentFusedSimilarityCandidate.project(candidate)(
            rrf_score=Rrf.score(candidate.lexical_rank, candidate.vector_rank, policy.rrf_k)
        )

    @step(input=scored_document_candidates, output=document_candidates)
    def publish_documents(self, candidate: DocumentFusedSimilarityCandidate) -> DocumentFusedSimilarityCandidate:
        return DocumentFusedSimilarityCandidate.project(candidate)


class FuseSimilarityParagraphs(Transform):
    """Deduplicate paragraph retrieval lanes and calculate their RRF score."""

    policy = input(VectorIndexPolicy)
    paragraph_lexical_candidates = input(ParagraphFusedSimilarityCandidate)
    paragraph_vector_candidates = input(ParagraphFusedSimilarityCandidate)
    merged_candidates = lane(ParagraphFusedSimilarityCandidate)
    fused_candidates = lane(ParagraphFusedSimilarityCandidate)
    scored_candidates = lane(ParagraphFusedSimilarityCandidate)
    paragraph_candidates = output(ParagraphFusedSimilarityCandidate)

    @step(input=[paragraph_lexical_candidates, paragraph_vector_candidates], output=merged_candidates)
    def merge_paragraphs(
        self, lexical: ParagraphFusedSimilarityCandidate, vector: ParagraphFusedSimilarityCandidate
    ) -> ParagraphFusedSimilarityCandidate:
        merged = union_all(vector)
        return ParagraphFusedSimilarityCandidate.project(merged)

    @step(input=[merged_candidates, policy], output=fused_candidates)
    def fuse_paragraphs(
        self, candidate: ParagraphFusedSimilarityCandidate, policy: VectorIndexPolicy
    ) -> ParagraphFusedSimilarityCandidate:
        param_join(policy)
        lexical_rank = max(candidate.lexical_rank)
        vector_rank = max(candidate.vector_rank)
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
            lexical_rank=lexical_rank,
            vector_rank=vector_rank,
            score_overlap=max(candidate.score_overlap),
            bm25_left_to_right=max(candidate.bm25_left_to_right),
            bm25_right_to_left=max(candidate.bm25_right_to_left),
            bm25_mean=max(candidate.bm25_mean),
            vector_similarity=max(candidate.vector_similarity),
            rrf_score=max(candidate.rrf_score),
            rrf_k=policy.rrf_k,
            experiment_id=policy.experiment_id,
        )

    @step(input=[fused_candidates, policy], output=scored_candidates)
    def score_paragraphs(
        self, candidate: ParagraphFusedSimilarityCandidate, policy: VectorIndexPolicy
    ) -> ParagraphFusedSimilarityCandidate:
        param_join(policy)
        return ParagraphFusedSimilarityCandidate.project(candidate)(
            rrf_score=Rrf.score(candidate.lexical_rank, candidate.vector_rank, policy.rrf_k)
        )

    @step(input=scored_candidates, output=paragraph_candidates)
    def publish_paragraphs(self, candidate: ParagraphFusedSimilarityCandidate) -> ParagraphFusedSimilarityCandidate:
        return ParagraphFusedSimilarityCandidate.project(candidate)
