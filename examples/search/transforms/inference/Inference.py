"""First-class query and document inference workflow."""

from typing import cast

from examples.search.inference.InferenceAdapter import InferenceAdapter
from examples.search.inference.InferenceAdapterRegistry import InferenceAdapterRegistry
from examples.search.schemas.indexing.vector import DocumentVectorEmbedding, SearchQueryVectorEmbedding
from examples.search.schemas.inference import (
    DocumentInferenceResult,
    DocumentInferenceStatus,
    InferencePolicy,
    QueryInferenceResult,
    QueryInferenceStatus,
)
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.text import Document
from structure import Transform, input, lane, output, parameter, step
from structure.plugin.pyspark import array_repeat, coalesce, param_join, where


class Inference(Transform):
    """Invoke the configured adapter and publish embeddings plus observable status."""

    adapter = parameter(InferenceAdapterRegistry.default())
    streaming = parameter(False)

    policy = input(InferencePolicy)
    queries = input(SearchQuery, streaming=True)
    documents = input(Document)

    query_inference = lane(QueryInferenceResult)
    document_inference = lane(DocumentInferenceResult)
    query_embeddings = output(SearchQueryVectorEmbedding)
    document_embeddings = output(DocumentVectorEmbedding)
    query_status = output(QueryInferenceStatus)
    document_status = output(DocumentInferenceStatus)

    @step(input=[queries, policy], output=query_inference)
    def infer_query(self, query: SearchQuery, policy: InferencePolicy) -> QueryInferenceResult:
        param_join(policy)
        return cast(InferenceAdapter, self.adapter).infer_query(query, policy, cast(bool, self.streaming))

    @step(input=[documents, policy], output=document_inference)
    def infer_document(self, document: Document, policy: InferencePolicy) -> DocumentInferenceResult:
        param_join(policy)
        return cast(InferenceAdapter, self.adapter).infer_document(
            document, policy, cast(bool, self.streaming)
        )

    @step(input=[query_inference, policy], output=query_embeddings)
    def publish_query_embedding(
        self, result: QueryInferenceResult, policy: InferencePolicy
    ) -> SearchQueryVectorEmbedding:
        param_join(policy)
        where((result.status == "success") & result.vector.is_not_null())
        return SearchQueryVectorEmbedding(
            query_id=result.query_id,
            vector=coalesce(result.vector, array_repeat(0.0, policy.dimension)),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
        )

    @step(input=[document_inference, policy], output=document_embeddings)
    def publish_document_embedding(
        self, result: DocumentInferenceResult, policy: InferencePolicy
    ) -> DocumentVectorEmbedding:
        param_join(policy)
        where((result.status == "success") & result.vector.is_not_null())
        return DocumentVectorEmbedding(
            document_id=result.document_id,
            vector=coalesce(result.vector, array_repeat(0.0, policy.dimension)),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
        )

    @step(input=[query_inference, policy], output=query_status)
    def publish_query_status(
        self, result: QueryInferenceResult, policy: InferencePolicy
    ) -> QueryInferenceStatus:
        param_join(policy)
        return QueryInferenceStatus(
            query_id=result.query_id,
            provider_id=policy.provider_id,
            model_id=policy.model_id,
            model_version=policy.model_version,
            status=result.status,
            error_code=result.error_code,
            diagnostic=result.diagnostic,
            inferred_at=policy.inferred_at,
        )

    @step(input=[document_inference, policy], output=document_status)
    def publish_document_status(
        self, result: DocumentInferenceResult, policy: InferencePolicy
    ) -> DocumentInferenceStatus:
        param_join(policy)
        return DocumentInferenceStatus(
            document_id=result.document_id,
            provider_id=policy.provider_id,
            model_id=policy.model_id,
            model_version=policy.model_version,
            status=result.status,
            error_code=result.error_code,
            diagnostic=result.diagnostic,
            inferred_at=policy.inferred_at,
        )
