"""Experiment-aware behavior evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.behavior import BehaviorRequest
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.experiment import Experiment
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.user import BandMembership
from examples.search.transforms.evaluation.with_all.search_docs.eval_doc_search_behavior import (
    EvaluateDocumentSearchBehavior as Super,
)
from structure import input, step
from structure.plugin.pyspark import inner_join, where


class EvaluateDocumentSearchBehavior(Super):
    """Evaluate an experiment."""

    experiments = input(Experiment)

    @step(output=Super.selected_requests)
    def select_requests(
        self,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
        batch: EvaluationBatch,
        params: EvaluationParams,
        experiment: Experiment,
    ) -> BehaviorRequest:
        """Select active experiment requests satisfying combined label and user-band filters."""

        selected = super().select_requests(query, request, band, batch, params)
        inner_join(on=experiment.experiment_id == selected.experiment_id)
        where(experiment.is_active)
        return BehaviorRequest.base(selected)
