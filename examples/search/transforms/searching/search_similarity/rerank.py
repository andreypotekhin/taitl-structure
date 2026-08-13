"""Rerank fused similarity candidates and present matching similarity."""

from examples.search.schemas.similarities.vector import DocumentFusedSimilarityCandidate
from examples.search.schemas.similarity import IndexedSimilarDocument, SimilarityFusionPolicy, SimilaritySearchQuery
from examples.search.schemas.text import Document
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import inner_join, param_join, row_number, where


class RerankSimilarity(Transform):
    """Join adopted candidates to similarity and apply deterministic final ranking."""

    query = input(SimilaritySearchQuery)
    documents = input(Document)
    document_candidates = input(DocumentFusedSimilarityCandidate)
    policy = input(SimilarityFusionPolicy)
    ranked_documents = lane(IndexedSimilarDocument)
    similar_documents = output(IndexedSimilarDocument)

    @step(input=[document_candidates, query, documents], output=ranked_documents)
    def rank_documents(
        self,
        candidate: DocumentFusedSimilarityCandidate,
        query: SimilaritySearchQuery,
        document: Document,
    ) -> IndexedSimilarDocument:
        inner_join(query, on=query.id == candidate.left_document_id)
        inner_join(document, on=document.id == candidate.right_document_id)
        return IndexedSimilarDocument.base(document)(
            search_query_id=query.id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.bm25_left_to_right,
            lexical_rank=candidate.lexical_rank,
            vector_rank=candidate.vector_rank,
            vector_similarity=candidate.vector_similarity,
            rrf_k=candidate.rrf_k,
            rrf_score=candidate.rrf_score,
            vector_backend=candidate.vector_backend,
            vector_model_id=candidate.vector_model_id,
            vector_dimension=candidate.vector_dimension,
            vector_content_revision=candidate.vector_content_revision,
            experiment_id=candidate.experiment_id,
            rank=row_number(
                partition_by=candidate.left_document_id,
                order_by=(
                    candidate.rrf_score.desc_nulls_last(),
                    candidate.vector_similarity.desc_nulls_last(),
                    candidate.bm25_mean.desc_nulls_last(),
                    candidate.right_document_id.asc_nulls_first(),
                ),
            ),
        )

    @step(input=[ranked_documents, policy], output=similar_documents)
    def limit_documents(
        self, candidate: IndexedSimilarDocument, policy: SimilarityFusionPolicy
    ) -> IndexedSimilarDocument:
        param_join(policy)
        where(candidate.rank <= policy.maximum_results)
        return IndexedSimilarDocument.project(candidate)
