"""Label-sliced judged document-ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import EvaluationQuery
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import SearchQuery
from examples.search.transforms.evaluation.search_docs.ranking.eval_ranking import (
    EvaluateDocumentRankingQuality as Super,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, where


class EvaluateDocumentRankingQuality(Super):
    """Evaluate one ranking run for queries selected by a label band."""

    params = input(EvaluationParams)

    @step(output=Super.evaluated_queries)
    def select_queries(
        self, query: SearchQuery, batch: EvaluationBatch, params: EvaluationParams
    ) -> EvaluationQuery:
        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        where(params.matches_query(query))
        return EvaluationQuery(
            window=batch.window,
            params=EvaluationParams(queryset=params.queryset, labels=params.labels, band_id=params.band_id),
            experiment_id="",
            band_id=None,
            search_query_id=query.id,
        )
