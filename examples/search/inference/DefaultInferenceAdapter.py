"""Deterministic local Search inference adapter."""

from examples.search.inference.InferenceAdapter import InferenceAdapter
from examples.search.schemas.inference import DocumentInferenceResult, InferencePolicy, QueryInferenceResult
from structure.plugin.pyspark import (
    arr_aggregate,
    arr_transform,
    literal,
    lower,
    sequence,
    split,
    sqrt,
    trim,
    when,
    xxhash64,
)


class DefaultInferenceAdapter(InferenceAdapter):
    """Deterministic normalized token/text-hash adapter for tests and development."""

    provider_id = "default"

    def _vector(self, content, policy: InferencePolicy):
        scale = 1.0 / sqrt(policy.dimension)
        tokens = split(lower(trim(content)), pattern="\\s+", limit=-1)
        positions = sequence(0, policy.dimension - 1)
        return arr_transform(
            positions,
            lambda position: when(
                arr_aggregate(
                    tokens,
                    0,
                    lambda total, token: total
                    + when((xxhash64(token, position) % 2) == 0, 1).otherwise(-1),
                )
                >= 0,
                scale,
            ).otherwise(-scale),
        )

    def infer_query(self, query, policy: InferencePolicy, streaming: bool) -> QueryInferenceResult:
        return QueryInferenceResult(
            query_id=query.id,
            vector=self._vector(query.content, policy),
            status=literal("success"),
            error_code=literal(None),
            diagnostic=literal(None),
        )

    def infer_document(self, document, policy: InferencePolicy, streaming: bool) -> DocumentInferenceResult:
        return DocumentInferenceResult(
            document_id=document.id,
            vector=self._vector(document.content, policy),
            status=literal("success"),
            error_code=literal(None),
            diagnostic=literal(None),
        )
