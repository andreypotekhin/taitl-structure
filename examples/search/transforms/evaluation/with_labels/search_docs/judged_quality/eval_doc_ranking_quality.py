"""Label-sliced judged document-ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import EvaluationQuery
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import SearchQuery
from examples.search.transforms.evaluation.search_docs.judged_quality import (
    EvaluateDocumentRankingQuality as BaseEvaluateDocumentRankingQuality,
)
from structure import input, step
from structure.plugin.pyspark import arr_exists, arr_forall, cross_join, element_at, where


class EvaluateDocumentRankingQuality(BaseEvaluateDocumentRankingQuality):
    """Evaluate one ranking run for queries selected by a label band."""

    params = input(EvaluationParams)

    @step(output=BaseEvaluateDocumentRankingQuality.evaluated_queries)
    def select_queries(
        self, query: SearchQuery, batch: EvaluationBatch, params: EvaluationParams
    ) -> EvaluationQuery:
        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        where(self._matches(query, params))
        return EvaluationQuery(
            window=batch.window,
            params=EvaluationParams(labels=params.labels),
            experiment_id="",
            search_query_id=query.id,
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
