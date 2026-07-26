"""Experiment-aware behavior evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.behavior import BehaviorRequest
from examples.search.schemas.experiment import Experiment
from examples.search.transforms.evaluation.search_docs.behavior.eval_doc_search_behavior import (
    EvaluateDocumentSearchBehavior as BaseEvaluateDocumentSearchBehavior,
)
from examples.search.transforms.evaluation.search_docs.behavior.eval_doc_search_behavior import (
    SelectDocumentSearchRequests,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, inner_join, where


class EvaluateDocumentSearchBehavior(BaseEvaluateDocumentSearchBehavior):
    """Evaluate requests belonging to current named experiments."""

    experiments = input(Experiment)

    @step(output=SelectDocumentSearchRequests.selected)
    def select_requests(
        self, request: SearchRequest, batch: EvaluationBatch, experiment: Experiment
    ) -> BehaviorRequest:
        cross_join(batch, allow_cartesian=True)
        inner_join(on=experiment.experiment_id == request.experiment_id)
        where(
            experiment.is_active,
            (request.requested_at >= batch.window.start) & (request.requested_at < batch.window.end),
        )
        return BehaviorRequest(
            window=batch.window,
            params=None,
            experiment_id=request.experiment_id,
            search_request_id=request.id,
            ranking_version=request.ranking_version,
            query=request.query,
        )
