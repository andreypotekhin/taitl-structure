"""Adopt lexical and vector document similarity candidates."""

from examples.search.algorithms.similarity.adapter import SimilarityCandidateAdapter
from examples.search.schemas.indexing.vector import DocumentVectorCandidate
from examples.search.schemas.similarities.vector import DocumentFusedSimilarityCandidate
from examples.search.schemas.similarity import DocumentSimilarity, SimilaritySearchQuery
from examples.search.schemas.text import Document
from structure import Transform, input, output, parameter, step
from structure.plugin.pyspark import inner_join, require_unique, where


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


class AdoptVectorSimilarity(Transform):
    """Translate exact vector retrieval results into same-grain candidates."""

    adapter = parameter(SimilarityCandidateAdapter())

    query = input(SimilaritySearchQuery)
    documents = input(Document)
    document_candidates = input(DocumentVectorCandidate)
    adopted_document_candidates = output(DocumentFusedSimilarityCandidate)

    @step(input=[document_candidates, query, documents], output=adopted_document_candidates)
    def adopt_documents(
        self, candidate: DocumentVectorCandidate, query: SimilaritySearchQuery, document: Document
    ) -> DocumentFusedSimilarityCandidate:
        inner_join(query, on=query.id == candidate.query_document_id)
        inner_join(document, on=document.id == candidate.document_id)
        where(candidate.query_document_id.is_not_null())
        require_unique(candidate.query_document_id, candidate.document_id)
        return self.adapter.document(candidate, query, document)
