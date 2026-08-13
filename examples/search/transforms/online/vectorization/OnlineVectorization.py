"""Fill request-time query and selected-document vector gaps."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.inference import *
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from examples.search.schemas.text import Document
from examples.search.transforms.online.vectorization.MergeDocumentVectors import MergeDocumentVectors
from examples.search.transforms.online.vectorization.MergeQueryEmbeddings import MergeQueryEmbeddings
from examples.search.transforms.online.vectorization.select_document_gaps import SelectDocumentGaps
from examples.search.transforms.online.vectorization.select_query_gaps import SelectQueryGaps
from examples.search.transforms.vectorization import Vectorization
from examples.search.transforms.vectorization.queries import VectorizeSearchQueries
from structure import Transform, input, output


class OnlineVectorization(Transform):
    """Resolve only request queries and documents admitted by filter targets."""

    queries = input(SearchQuery, streaming=True)
    documents = input(Document)
    cached_query_embeddings = input(SearchQueryVectorEmbedding, streaming=True)
    document_vector_index = input(DocumentVectorIndex)
    document_targets = input(DocumentSearchTarget, streaming=True)
    inference_policy = input(InferencePolicy)
    vector_policy = input(VectorIndexPolicy)

    query_gaps = SelectQueryGaps(
        queries=queries,
        embeddings=cached_query_embeddings,
        policy=inference_policy,
    )

    document_gaps = SelectDocumentGaps(
        targets=document_targets,
        documents=documents,
        document_index=document_vector_index,
        policy=inference_policy,
    )

    vectorized = Vectorization(
        queries=query_gaps.gaps,
        documents=document_gaps.gaps,
        inference_policy=inference_policy,
        streaming_mode=True,
    )

    merged_queries = MergeQueryEmbeddings(
        cached=cached_query_embeddings,
        inferred=vectorized.query_embeddings,
        policy=inference_policy,
    )
    merged_documents = MergeDocumentVectors(
        cached=document_vector_index,
        inferred=vectorized.document_embeddings,
        policy=vector_policy,
    )
    query_vectors = VectorizeSearchQueries(
        queries=queries,
        embeddings=merged_queries.embeddings,
    )

    query_embeddings = output(SearchQueryVectorEmbedding, merged_queries.embeddings)
    document_embeddings = output(DocumentVectorIndex, merged_documents.embeddings)
    vector_queries = output(DocumentVectorQuery, query_vectors.vector_queries)
    query_inference_status = output(QueryInferenceStatus, vectorized.query_inference_status)
    document_inference_status = output(DocumentInferenceStatus, vectorized.document_inference_status)
