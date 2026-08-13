"""Adopt ranked lexical similarity pairs into the hybrid candidate contract."""

from examples.search.schemas.similarities.vector import DocumentFusedSimilarityCandidate
from examples.search.schemas.similarity import DocumentSimilarity
from structure import Transform, input, output, step
from structure.plugin.pyspark import require_unique


class AdoptLexicalSimilarity(Transform):
    """Translate existing lexical similarity results into retrieval candidates."""

    document_similarities = input(DocumentSimilarity)
    document_candidates = output(DocumentFusedSimilarityCandidate)

    @step(input=document_similarities, output=document_candidates)
    def adopt_documents(self, pair: DocumentSimilarity) -> DocumentFusedSimilarityCandidate:
        require_unique(pair.left_document_id, pair.right_document_id)
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
            rrf_score=0.0,
            rrf_k=0,
            experiment_id="",
            vector_backend=None,
            vector_model_id=None,
            vector_dimension=None,
            vector_content_revision=None,
        )
