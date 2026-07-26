"""Experiment-aware judged ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import (
    DocumentRelevanceJudgment,
    EvaluationQuery,
    EvaluationResult,
)
from examples.search.schemas.experiment import Experiment
from examples.search.schemas.search import DocumentSearchResult, SearchQuery
from examples.search.transforms.evaluation.search_docs.judged_quality.eval_doc_ranking_quality import (
    EvaluateDocumentRankingQuality as BaseEvaluateDocumentRankingQuality,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, left_join, where


class EvaluateDocumentRankingQuality(BaseEvaluateDocumentRankingQuality):
    """Evaluate result rows belonging to current named experiments."""

    experiments = input(Experiment)

    @step(output=BaseEvaluateDocumentRankingQuality.evaluated_queries)
    def select_queries(self, query: SearchQuery, batch: EvaluationBatch, experiment: Experiment) -> EvaluationQuery:
        cross_join(batch, allow_cartesian=True)
        cross_join(experiment, allow_cartesian=True)
        where(experiment.is_active)
        return EvaluationQuery(
            window=batch.window,
            params=None,
            experiment_id=experiment.experiment_id,
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
            on=(result.search_query_id == query.search_query_id) & (result.experiment_id == query.experiment_id)
        )
        left_join(
            on=(judgment.search_query_id == query.search_query_id) & (judgment.document_id == result.document_id)
        )
        return EvaluationResult.base(query, result, judgment)
