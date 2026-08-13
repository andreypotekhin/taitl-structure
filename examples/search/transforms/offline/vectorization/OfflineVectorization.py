"""Build reusable document and query embeddings for an offline snapshot."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.inference import *
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from examples.search.schemas.text import Document
from examples.search.transforms.offline.vectorization.all_document_targets import AllDocumentTargets
from examples.search.transforms.online.vectorization import MergeDocumentVectors, MergeQueryEmbeddings
from examples.search.transforms.vectorization import Vectorization
from examples.search.transforms.vectorization.queries import VectorizeSearchQueries
from structure import Transform, input, output


class OfflineVectorization(Transform):
    """Infer and merge the complete offline query/document vector snapshot."""

    queries = input(SearchQuery)
    documents = input(Document)
    query_embeddings = input(SearchQueryVectorEmbedding)
    document_vector_index = input(DocumentVectorIndex)
    inference_policy = input(InferencePolicy)
    vector_policy = input(VectorIndexPolicy)

    targets = AllDocumentTargets(documents=documents)
    vectorized = Vectorization(
        queries=queries,
        documents=documents,
        inference_policy=inference_policy,
        streaming_mode=False,
    )

    merged_queries = MergeQueryEmbeddings(
        cached=query_embeddings,
        inferred=vectorized.query_embeddings,
        policy=inference_policy,
    )
    merged_documents = MergeDocumentVectors(
        cached=document_vector_index,
        inferred=vectorized.document_embeddings,
        policy=vector_policy,
    )
    vectorized_queries = VectorizeSearchQueries(
        queries=queries,
        embeddings=merged_queries.embeddings,
    )

    vector_queries = output(DocumentVectorQuery, vectorized_queries.vector_queries)
    document_embeddings = output(DocumentVectorIndex, merged_documents.embeddings)
    query_embeddings_out = output(SearchQueryVectorEmbedding, merged_queries.embeddings)
    query_inference_status = output(QueryInferenceStatus, vectorized.query_inference_status)
    document_inference_status = output(DocumentInferenceStatus, vectorized.document_inference_status)
    document_targets = output(DocumentSearchTarget, targets.targets)
