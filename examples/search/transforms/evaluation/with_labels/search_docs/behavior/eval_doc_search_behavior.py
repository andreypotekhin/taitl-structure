"""Label-sliced observed document-search behavior evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.behavior import BehaviorRequest
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import SearchQuery
from examples.search.transforms.evaluation.search_docs.behavior.eval_doc_search_behavior import (
    EvaluateDocumentSearchBehavior as BaseEvaluateDocumentSearchBehavior,
    SelectDocumentSearchRequests,
)
from structure import input, step
from structure.plugin.pyspark import arr_exists, arr_forall, cross_join, element_at, inner_join, where


class EvaluateDocumentSearchBehavior(BaseEvaluateDocumentSearchBehavior):
    """Evaluate observed behavior for requests selected by a query label band."""

    queries = input(SearchQuery)
    params = input(EvaluationParams)

    @step(output=SelectDocumentSearchRequests.selected)
    def select_requests(
        self, request: SearchRequest, query: SearchQuery, batch: EvaluationBatch, params: EvaluationParams
    ) -> BehaviorRequest:
        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=query.id == request.query_id)
        where(
            self._matches(query, params),
            (request.requested_at >= batch.window.start) & (request.requested_at < batch.window.end),
        )
        return BehaviorRequest(
            window=batch.window,
            params=EvaluationParams(labels=params.labels),
            experiment_id=request.experiment_id,
            search_request_id=request.id,
            ranking_version=request.ranking_version,
            query=request.query,
        )

    @staticmethod
    def _matches(query: SearchQuery, params: EvaluationParams):
        return arr_forall(
            params.labels,
            lambda requested: arr_exists(
                params.labels,
                lambda candidate: (candidate.name == requested.name)
                & (element_at(query.labels, candidate.name) == candidate.value),
                argument_name="candidate",
            ),
            argument_name="requested",
        )
