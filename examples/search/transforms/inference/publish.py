"""Publish validated inference embeddings and observable statuses."""

from examples.search.schemas.indexing.vector import DocumentVectorEmbedding, SearchQueryVectorEmbedding
from examples.search.schemas.inference import (
    DocumentInferenceResult,
    DocumentInferenceStatus,
    InferencePolicy,
    QueryInferenceResult,
    QueryInferenceStatus,
)
from structure import Transform, input, output, step
from structure.plugin.pyspark import array_repeat, coalesce, param_join, where


class PublishQueryInference(Transform):
    """Publish query embeddings and statuses from adapter results."""

    policy = input(InferencePolicy)
    results = input(QueryInferenceResult)
    embeddings = output(SearchQueryVectorEmbedding)
    statuses = output(QueryInferenceStatus)

    @step(input=[results, policy], output=embeddings)
    def embedding(self, result: QueryInferenceResult, policy: InferencePolicy) -> SearchQueryVectorEmbedding:
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

    @step(input=[results, policy], output=statuses)
    def status(self, result: QueryInferenceResult, policy: InferencePolicy) -> QueryInferenceStatus:
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


class PublishDocumentInference(Transform):
    """Publish document embeddings and statuses from adapter results."""

    policy = input(InferencePolicy)
    results = input(DocumentInferenceResult)
    embeddings = output(DocumentVectorEmbedding)
    statuses = output(DocumentInferenceStatus)

    @step(input=[results, policy], output=embeddings)
    def embedding(self, result: DocumentInferenceResult, policy: InferencePolicy) -> DocumentVectorEmbedding:
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

    @step(input=[results, policy], output=statuses)
    def status(self, result: DocumentInferenceResult, policy: InferencePolicy) -> DocumentInferenceStatus:
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
