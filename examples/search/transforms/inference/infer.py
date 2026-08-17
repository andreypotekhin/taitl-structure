"""Invoke the configured Search inference adapter."""

from typing import cast

from examples.search.inference.InferenceAdapter import InferenceAdapter
from examples.search.inference.InferenceAdapterRegistry import InferenceAdapterRegistry
from examples.search.schemas.inference import DocumentInferenceResult, InferencePolicy, QueryInferenceResult
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.text import Document
from structure import Transform, input, output, parameter, step
from structure.plugin.pyspark import param_join


class InferQueries(Transform):
    """Invoke the configured adapter for SearchQuery rows."""

    adapter = parameter(InferenceAdapterRegistry.default())
    streaming = parameter(False)

    policy = input(InferencePolicy)
    queries = input(SearchQuery, streaming=True)
    results = output(QueryInferenceResult)

    @step(input=[queries, policy], output=results)
    def infer(self, query: SearchQuery, policy: InferencePolicy) -> QueryInferenceResult:
        param_join(policy)
        return cast(InferenceAdapter, self.adapter).infer_query(query, policy, cast(bool, self.streaming))


class InferDocuments(Transform):
    """Invoke the configured adapter for Document rows."""

    adapter = parameter(InferenceAdapterRegistry.default())
    streaming = parameter(False)

    policy = input(InferencePolicy)
    documents = input(Document)
    results = output(DocumentInferenceResult)

    @step(input=[documents, policy], output=results)
    def infer(self, document: Document, policy: InferencePolicy) -> DocumentInferenceResult:
        param_join(policy)
        return cast(InferenceAdapter, self.adapter).infer_document(document, policy, cast(bool, self.streaming))
