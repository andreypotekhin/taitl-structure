"""Adopt ranked vector candidates into the hybrid similarity contract."""

from examples.search.schemas.indexing.vector import DocumentVectorCandidate, ParagraphVectorCandidate
from examples.search.schemas.similarities.vector import (
    DocumentFusedSimilarityCandidate,
    ParagraphFusedSimilarityCandidate,
)
from structure import Transform, input, output, step
from structure.plugin.pyspark import require_unique


class AdoptVectorSimilarity(Transform):
    """Translate exact vector retrieval results into same-grain candidates."""

    document_candidates = input(DocumentVectorCandidate)
    adopted_document_candidates = output(DocumentFusedSimilarityCandidate)

    @step(input=document_candidates, output=adopted_document_candidates)
    def adopt_documents(self, candidate: DocumentVectorCandidate) -> DocumentFusedSimilarityCandidate:
        require_unique(candidate.query_document_id, candidate.document_id)
        return DocumentFusedSimilarityCandidate(
            left_document_id=candidate.query_document_id,
            right_document_id=candidate.document_id,
            lexical_rank=None,
            vector_rank=candidate.rank,
            score_overlap=None,
            bm25_left_to_right=None,
            bm25_right_to_left=None,
            bm25_mean=None,
            vector_similarity=candidate.cosine_similarity,
            rrf_score=0.0,
            rrf_k=0,
            experiment_id="",
            vector_backend=candidate.vector_backend,
            vector_model_id=candidate.model_id,
            vector_dimension=candidate.dimension,
            vector_content_revision=candidate.content_revision,
        )

class AdoptVectorParagraphs(Transform):
    """Translate exact vector paragraph results into retrieval candidates."""

    paragraph_candidates = input(ParagraphVectorCandidate)
    adopted_paragraph_candidates = output(ParagraphFusedSimilarityCandidate)

    @step(input=paragraph_candidates, output=adopted_paragraph_candidates)
    def adopt_paragraphs(self, candidate: ParagraphVectorCandidate) -> ParagraphFusedSimilarityCandidate:
        require_unique(
            candidate.query_document_id,
            candidate.query_section_id,
            candidate.query_paragraph_id,
            candidate.document_id,
            candidate.section_id,
            candidate.paragraph_id,
        )
        return ParagraphFusedSimilarityCandidate(
            left_document_id=candidate.query_document_id,
            left_section_id=candidate.query_section_id,
            left_paragraph_id=candidate.query_paragraph_id,
            right_document_id=candidate.document_id,
            right_section_id=candidate.section_id,
            right_paragraph_id=candidate.paragraph_id,
            lexical_rank=None,
            vector_rank=candidate.rank,
            score_overlap=None,
            bm25_left_to_right=None,
            bm25_right_to_left=None,
            bm25_mean=None,
            vector_similarity=candidate.cosine_similarity,
            rrf_score=0.0,
            rrf_k=0,
            experiment_id="",
            vector_backend=candidate.vector_backend,
            vector_model_id=candidate.model_id,
            vector_dimension=candidate.dimension,
            vector_content_revision=candidate.content_revision,
        )
