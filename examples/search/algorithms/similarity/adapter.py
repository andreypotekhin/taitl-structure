"""Replaceable adapters for normalizing provider-neutral similarity candidates."""

from examples.search.schemas.indexing.vector import DocumentVectorCandidate, ParagraphVectorCandidate
from examples.search.schemas.similarities.vector import (
    DocumentFusedSimilarityCandidate,
    ParagraphFusedSimilarityCandidate,
)
from examples.search.schemas.similarity import SimilaritySearchQuery
from examples.search.schemas.text import Document, Paragraph


class SimilarityCandidateAdapter:
    """Map provider-neutral ranked candidates into the fusion contract.

    Callers may subclass this adapter when their example-owned provider needs to
    add or normalize provenance before the shared fusion stages consume it.
    Candidate production itself remains a separate transform boundary.
    """

    def document(
        self,
        candidate: DocumentVectorCandidate,
        query: SimilaritySearchQuery,
        document: Document,
    ) -> DocumentFusedSimilarityCandidate:
        return DocumentFusedSimilarityCandidate(
            left_document_id=query.id,
            right_document_id=document.id,
            lexical_rank=None,
            vector_rank=candidate.rank,
            score_overlap=None,
            bm25_left_to_right=None,
            bm25_right_to_left=None,
            bm25_mean=None,
            vector_similarity=candidate.cosine_similarity,
            vector_backend=candidate.vector_backend,
            vector_model_id=candidate.model_id,
            vector_dimension=candidate.dimension,
            vector_content_revision=candidate.content_revision,
            rrf_score=0.0,
            rrf_k=0,
            experiment_id=candidate.experiment_id,
        )

    def paragraph(
        self,
        candidate: ParagraphVectorCandidate,
        query: Paragraph,
        paragraph: Paragraph,
    ) -> ParagraphFusedSimilarityCandidate:
        return ParagraphFusedSimilarityCandidate(
            left_document_id=query.document_id,
            left_section_id=query.section_id,
            left_paragraph_id=query.id,
            right_document_id=paragraph.document_id,
            right_section_id=paragraph.section_id,
            right_paragraph_id=paragraph.id,
            lexical_rank=None,
            vector_rank=candidate.rank,
            score_overlap=None,
            bm25_left_to_right=None,
            bm25_right_to_left=None,
            bm25_mean=None,
            vector_similarity=candidate.cosine_similarity,
            vector_backend=candidate.vector_backend,
            vector_model_id=candidate.model_id,
            vector_dimension=candidate.dimension,
            vector_content_revision=candidate.content_revision,
            rrf_score=0.0,
            rrf_k=0,
            experiment_id=candidate.experiment_id,
        )
