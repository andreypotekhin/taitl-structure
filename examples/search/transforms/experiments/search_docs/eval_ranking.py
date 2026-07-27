"""Experiment-aware judged ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import (
    DocumentRelevanceJudgment,
    EvaluationQuery,
    EvaluationResult,
)
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.experiment import Experiment
from examples.search.schemas.search import DocumentSearchResult, SearchQuery
from examples.search.transforms.evaluation.search_docs.ranking.with_all import EvaluateDocumentRankingQuality as Super
from structure import input, step
from structure.plugin.pyspark import cross_join, inner_join, left_join, where


class EvaluateDocumentRankingQuality(Super):
    """Evaluate an experiment."""

    experiments = input(Experiment)

    @step(output=Super.evaluated_queries)
    def select_queries(
        self,
        query: SearchQuery,
        result: DocumentSearchResult,
        batch: EvaluationBatch,
        params: EvaluationParams,
        experiment: Experiment,
    ) -> EvaluationQuery:
        """Select active experiment rows satisfying both label and user-band filters."""

        selected = super().select_queries(query, result, batch, params)
        cross_join(experiment, allow_cartesian=True)
        where(experiment.is_active)
        return EvaluationQuery(
            window=selected.window,
            params=selected.params,
            experiment_id=experiment.experiment_id,
            band_id=selected.band_id,
            search_query_id=selected.search_query_id,
        )

    @step(output=Super.evaluated_results)
    def select_results(
        self,
        query: EvaluationQuery,
        result: DocumentSearchResult,
        judgment: DocumentRelevanceJudgment,
    ) -> EvaluationResult:
        left_join(
            on=(result.search_query_id == query.search_query_id)
            & (result.experiment_id == query.experiment_id)
            & result.band_id.null_safe_eq(query.band_id)
        )
        left_join(on=(judgment.search_query_id == query.search_query_id) & (judgment.document_id == result.document_id))
        return EvaluationResult.project(query, result, judgment)(
            experiment_id=query.experiment_id,
            band_id=query.band_id,
            search_query_id=query.search_query_id,
            document_id=result.document_id,
        )
