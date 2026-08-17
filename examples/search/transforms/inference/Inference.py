"""First-class query and document inference workflow."""

from examples.search.inference.InferenceAdapterRegistry import InferenceAdapterRegistry
from examples.search.schemas.indexing.vector import DocumentVectorEmbedding, SearchQueryVectorEmbedding
from examples.search.schemas.inference import DocumentInferenceStatus, InferencePolicy, QueryInferenceStatus
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.text import Document
from examples.search.transforms.inference.infer import InferDocuments, InferQueries
from examples.search.transforms.inference.publish import PublishDocumentInference, PublishQueryInference
from structure import Transform, input, output, parameter


class Inference(Transform):
    """Invoke the configured adapter and publish embeddings plus observable status."""

    adapter = parameter(InferenceAdapterRegistry.default())
    streaming = parameter(False)

    policy = input(InferencePolicy)
    queries = input(SearchQuery, streaming=True)
    documents = input(Document)

    inferred_queries = InferQueries(adapter=adapter, streaming=streaming, policy=policy, queries=queries)
    inferred_documents = InferDocuments(adapter=adapter, streaming=streaming, policy=policy, documents=documents)

    published_queries = PublishQueryInference(policy=policy, results=inferred_queries.results)
    published_documents = PublishDocumentInference(policy=policy, results=inferred_documents.results)

    query_embeddings = output(SearchQueryVectorEmbedding, published_queries.embeddings)
    document_embeddings = output(DocumentVectorEmbedding, published_documents.embeddings)
    query_status = output(QueryInferenceStatus, published_queries.statuses)
    document_status = output(DocumentInferenceStatus, published_documents.statuses)
