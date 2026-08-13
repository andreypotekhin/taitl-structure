"""Adopt ranked vector candidates into the hybrid similarity contract."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.similarities.vector import *
from examples.search.schemas.similarity import *
from examples.search.schemas.text import *
from structure import *
from structure.plugin.pyspark import *


class AdoptVectorSimilarity(Transform):
    """Translate exact vector retrieval results into same-grain candidates."""

    query = input(SimilaritySearchQuery)
    documents = input(Document)
    document_scores = input(DocumentVectorScore)
    adopted_document_candidates = output(DocumentFusedSimilarityCandidate)

    @step(input=[document_scores, query, documents], output=adopted_document_candidates)
    def adopt_documents(
        self, score: DocumentVectorScore, query: SimilaritySearchQuery, document: Document
    ) -> DocumentFusedSimilarityCandidate:
        inner_join(query, on=query.id == score.query_id)
        inner_join(document, on=document.id == score.document_id)
        where(score.query_document_id.is_not_null())
        where(score.query_document_id == query.id)
        require_unique(score.query_document_id, score.document_id)
        return DocumentFusedSimilarityCandidate(
            left_document_id=score.query_document_id,
            right_document_id=document.id,
            lexical_rank=None,
            vector_rank=row_number(
                partition_by=score.query_id,
                order_by=(score.cosine_similarity.desc_nulls_last(), document.id.asc_nulls_first()),
            ),
            score_overlap=None,
            bm25_left_to_right=None,
            bm25_right_to_left=None,
            bm25_mean=None,
            vector_similarity=score.cosine_similarity,
            rrf_score=0.0,
            rrf_k=0,
            experiment_id="",
            vector_backend=score.vector_backend,
            vector_model_id=score.model_id,
            vector_dimension=score.dimension,
            vector_content_revision=score.content_revision,
        )
