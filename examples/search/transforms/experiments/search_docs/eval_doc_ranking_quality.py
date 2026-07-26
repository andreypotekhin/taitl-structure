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
from examples.search.transforms.evaluation.with_all.search_docs.eval_doc_ranking_quality import (
    EvaluateDocumentRankingQuality as BaseEvaluateDocumentRankingQuality,
)
from examples.search.transforms.evaluation.with_labels.search_docs.eval_doc_ranking_quality import (
    EvaluateDocumentRankingQuality as LabelSelection,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, group_by, inner_join, left_join, where


class EvaluateDocumentRankingQuality(BaseEvaluateDocumentRankingQuality):
    """Evaluate an experiment."""

    experiments = input(Experiment)
    _matches = staticmethod(LabelSelection._matches)

    @step(output=BaseEvaluateDocumentRankingQuality.evaluated_queries)
    def select_queries(
        self,
        query: SearchQuery,
        result: DocumentSearchResult,
        batch: EvaluationBatch,
        params: EvaluationParams,
        experiment: Experiment,
    ) -> EvaluationQuery:
        """Select active experiment rows satisfying both label and user-band filters."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        cross_join(experiment, allow_cartesian=True)
        inner_join(on=result.search_query_id == query.id)
        where(
            experiment.is_active,
            params.band_id.is_not_null(),
            result.band_id == params.band_id,
            self._matches(query, params),
        )
        group_by(
            window=batch.window,
            params=params,
            experiment_id=experiment.experiment_id,
            band_id=result.band_id,
            search_query_id=query.id,
        )
        return EvaluationQuery(
            window=batch.window,
            params=EvaluationParams(labels=params.labels, band_id=params.band_id),
            experiment_id=experiment.experiment_id,
            band_id=result.band_id,
            search_query_id=query.id,
        )

    @step(output=BaseEvaluateDocumentRankingQuality.evaluated_results)
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
        left_join(
            on=(judgment.search_query_id == query.search_query_id) & (judgment.document_id == result.document_id)
        )
        return EvaluationResult.base(query, result, judgment)
