"""Adopt ranked lexical similarity pairs into the hybrid candidate contract."""

from examples.search.schemas.similarities.vector import (
    DocumentFusedSimilarityCandidate,
    ParagraphFusedSimilarityCandidate,
)
from examples.search.schemas.similarity import DocumentSimilarity, ParagraphSimilarity
from structure import Transform, input, output, parameter, step


class AdoptLexicalSimilarity(Transform):
    """Translate existing lexical similarity results into retrieval candidates."""

    rrf_k = parameter(60)

    document_similarities = input(DocumentSimilarity)
    document_candidates = output(DocumentFusedSimilarityCandidate)

    @step(input=document_similarities, output=document_candidates)
    def adopt_documents(self, pair: DocumentSimilarity) -> DocumentFusedSimilarityCandidate:
        return DocumentFusedSimilarityCandidate(
            left_document_id=pair.left_document_id,
            right_document_id=pair.right_document_id,
            lexical_rank=pair.rank,
            vector_rank=None,
            score_overlap=pair.score_overlap,
            bm25_left_to_right=pair.bm25_left_to_right,
            bm25_right_to_left=pair.bm25_right_to_left,
            bm25_mean=pair.bm25_mean,
            vector_similarity=None,
            rrf_score=1.0 / (self.rrf_k + pair.rank),
            rrf_k=self.rrf_k,
            experiment_id="",
        )

class AdoptLexicalParagraphs(Transform):
    """Translate lexical paragraph similarity results into retrieval candidates."""

    rrf_k = parameter(60)

    paragraph_similarities = input(ParagraphSimilarity)
    paragraph_candidates = output(ParagraphFusedSimilarityCandidate)

    @step(input=paragraph_similarities, output=paragraph_candidates)
    def adopt_paragraphs(self, pair: ParagraphSimilarity) -> ParagraphFusedSimilarityCandidate:
        return ParagraphFusedSimilarityCandidate(
            left_document_id=pair.left_document_id,
            left_section_id=pair.left_section_id,
            left_paragraph_id=pair.left_paragraph_id,
            right_document_id=pair.right_document_id,
            right_section_id=pair.right_section_id,
            right_paragraph_id=pair.right_paragraph_id,
            lexical_rank=pair.rank,
            vector_rank=None,
            score_overlap=pair.score_overlap,
            bm25_left_to_right=pair.bm25_left_to_right,
            bm25_right_to_left=pair.bm25_right_to_left,
            bm25_mean=pair.bm25_mean,
            vector_similarity=None,
            rrf_score=1.0 / (self.rrf_k + pair.rank),
            rrf_k=self.rrf_k,
            experiment_id="",
        )
