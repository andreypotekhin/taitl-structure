"""Rerank fused similarity candidates and present matching documents."""

from typing import Final

from examples.search.schemas.similarities.vector import DocumentFusedSimilarityCandidate
from examples.search.schemas.similarity import IndexedSimilarDocument, SimilarityDocumentQuery
from examples.search.schemas.text import Document
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import inner_join, row_number, where


class RerankSimilarity(Transform):
    """Join adopted candidates to documents and apply deterministic final ranking."""

    maximum_results: Final = 10

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_candidates = input(DocumentFusedSimilarityCandidate)
    ranked_documents = lane(IndexedSimilarDocument)
    similar_documents = output(IndexedSimilarDocument)

    @step(input=[document_candidates, query, documents], output=ranked_documents)
    def rank_documents(
        self,
        candidate: DocumentFusedSimilarityCandidate,
        query: SimilarityDocumentQuery,
        document: Document,
    ) -> IndexedSimilarDocument:
        inner_join(query, on=query.id == candidate.left_document_id)
        inner_join(document, on=document.id == candidate.right_document_id)
        return IndexedSimilarDocument.base(document)(
            search_query_id=query.id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.bm25_left_to_right,
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

    @step(input=ranked_documents, output=similar_documents)
    def limit_documents(self, candidate: IndexedSimilarDocument) -> IndexedSimilarDocument:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarDocument.project(candidate)
