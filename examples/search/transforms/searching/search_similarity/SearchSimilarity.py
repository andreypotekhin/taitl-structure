"""Rank corpus documents by their directed similarity to one query document."""

from typing import Final

from examples.search.schemas.similarity import DocumentSimilarity as DocumentSimilarityPair
from examples.search.schemas.similarity import IndexedSimilarDocument, SimilarityDocumentQuery
from examples.search.schemas.text import Document
from structure import Transform, input, lane, output
from structure.plugin.pyspark import inner_join, where


class SearchSimilarity(Transform):
    """Return the top fixed number of corpus documents similar to one query document."""

    maximum_results: Final = 10

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_similarities = input(DocumentSimilarityPair)
    ranked_documents = lane(IndexedSimilarDocument)
    similar_documents = output(IndexedSimilarDocument)

    def rank(
        self, query: SimilarityDocumentQuery, document: Document, pair: DocumentSimilarityPair
    ) -> IndexedSimilarDocument:
        inner_join(on=query.id == pair.left_document_id)
        candidate_id = pair.right_document_id
        score_bm25 = pair.bm25_left_to_right
        inner_join(on=document.id == candidate_id)
        return IndexedSimilarDocument.base(document)(
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=pair.rank,
        )

    def limit(self, candidate: IndexedSimilarDocument) -> IndexedSimilarDocument:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarDocument.project(candidate)
