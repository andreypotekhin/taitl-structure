"""User-band-sliced judged document-ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import EvaluationQuery
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import DocumentSearchResult, SearchQuery
from examples.search.transforms.evaluation.search_docs.ranking.eval_ranking import (
    EvaluateDocumentRankingQuality as Super,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, group_by, inner_join, where


class EvaluateDocumentRankingQuality(Super):
    """Evaluate context-specific rankings for users matching one persisted band."""

    params = input(EvaluationParams)

    @step(output=Super.evaluated_queries)
    def select_queries(
        self,
        query: SearchQuery,
        result: DocumentSearchResult,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> EvaluationQuery:
        """Select the already materialized policy for one requested band."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=result.search_query_id == query.id)
        where(params.matches_band(result.band_id), params.matches_queryset(query))
        group_by(
            window=batch.window,
            params=params,
            experiment_id=None,
            band_id=result.band_id,
            search_query_id=query.id,
        )
        return EvaluationQuery(
            window=batch.window,
            params=EvaluationParams(queryset=params.queryset, labels=params.labels, band_id=params.band_id),
            experiment_id=None,
            band_id=result.band_id,
            search_query_id=query.id,
        )
