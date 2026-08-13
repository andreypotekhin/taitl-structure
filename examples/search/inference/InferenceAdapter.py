"""Replaceable provider adapter contract for compiler-visible Search inference."""

from __future__ import annotations

from examples.search.schemas.inference import DocumentInferenceResult, InferencePolicy, QueryInferenceResult


class InferenceAdapter:
    """Adapter contract for query and document vector expressions.

    Implementations may use explicit Spark-compatible provider expressions or UDFs.
    The ``streaming`` flag lets an implementation select a streaming-appropriate
    execution strategy while preserving one result contract.
    """

    provider_id = "default"

    def infer_query(self, query, policy: InferencePolicy, streaming: bool) -> QueryInferenceResult:
        raise NotImplementedError

    def infer_document(self, document, policy: InferencePolicy, streaming: bool) -> DocumentInferenceResult:
        raise NotImplementedError
