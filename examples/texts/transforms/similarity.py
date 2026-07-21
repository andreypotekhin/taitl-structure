"""Rank documents similar to one query document."""

from typing import Final

from examples.texts.schemas.similarity import DocumentSimilarity as DocumentSimilarityPair
from examples.texts.schemas.similarity import IndexedSimilarDocument, SimilarityDocumentQuery
from examples.texts.schemas.text import Document
from examples.texts.transforms.similarities.SimilarParagraphs import SimilarParagraphs
from examples.texts.transforms.similarities.SimilarSections import SimilarSections
from examples.texts.transforms.similarities.SimilarSentences import SimilarSentences
from structure import *
from structure.plugin.pyspark import *


class Similarity(Transform):
    """Return the top fixed number of corpus documents similar to one query document."""

    maximum_results: Final = 10

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_similarities = input(DocumentSimilarityPair)
    ranked_documents = lane(IndexedSimilarDocument)
    similar_documents = output(IndexedSimilarDocument)

    @step(input=[query, documents, document_similarities], output=ranked_documents)
    def rank(
        self, query: SimilarityDocumentQuery, document: Document, pair: DocumentSimilarityPair
    ) -> IndexedSimilarDocument:
        inner_join(on=(query.id == pair.left_document_id) | (query.id == pair.right_document_id))
        candidate_id = when(query.id == pair.left_document_id, pair.right_document_id).otherwise(pair.left_document_id)
        score_bm25 = when(query.id == pair.left_document_id, pair.bm25_left_to_right).otherwise(pair.bm25_right_to_left)
        inner_join(on=document.id == candidate_id)
        rank = row_number(
            partition_by=query.id,
            order_by=(score_bm25.desc(), pair.score_overlap.desc(), document.id.asc()),
        )
        return IndexedSimilarDocument(
            id=document.id,
            collection_id=document.collection_id,
            source=document.source,
            title=document.title,
            url=document.url,
            content=document.content,
            content_type=document.content_type,
            encoding=document.encoding,
            language=document.language,
            created_at=document.created_at,
            published_at=document.published_at,
            harvested_at=document.harvested_at,
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=rank,
        )

    @step(input=ranked_documents, output=similar_documents)
    def limit(self, candidate: IndexedSimilarDocument) -> IndexedSimilarDocument:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarDocument(
            id=candidate.id,
            collection_id=candidate.collection_id,
            source=candidate.source,
            title=candidate.title,
            url=candidate.url,
            content=candidate.content,
            content_type=candidate.content_type,
            encoding=candidate.encoding,
            language=candidate.language,
            created_at=candidate.created_at,
            published_at=candidate.published_at,
            harvested_at=candidate.harvested_at,
            search_query_id=candidate.search_query_id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.score_bm25,
            rank=candidate.rank,
        )
