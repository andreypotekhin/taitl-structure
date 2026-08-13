"""Infer vectors for query and document relations supplied by a facet."""

from examples.search.schemas.indexing.vector import DocumentVectorEmbedding, SearchQueryVectorEmbedding
from examples.search.schemas.inference import DocumentInferenceStatus, InferencePolicy, QueryInferenceStatus
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.text import Document
from examples.search.transforms.inference import Inference
from structure import Transform, input, output, parameter


class Vectorization(Transform):
    """Invoke inference for the query and document relations supplied by a facet."""

    queries = input(SearchQuery, streaming=True)
    documents = input(Document)
    inference_policy = input(InferencePolicy)
    streaming_mode = parameter(False)

    inferred = Inference(
        queries=queries,
        documents=documents,
        policy=inference_policy,
        streaming=streaming_mode,
    )

    query_embeddings = output(SearchQueryVectorEmbedding, inferred.query_embeddings)
    document_embeddings = output(DocumentVectorEmbedding, inferred.document_embeddings)
    query_inference_status = output(QueryInferenceStatus, inferred.query_status)
    document_inference_status = output(DocumentInferenceStatus, inferred.document_status)
