"""Combined label-and-user-band judged document-ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import EvaluationQuery
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import DocumentSearchResult, SearchQuery
from examples.search.transforms.evaluation.search_docs.ranking.with_users import EvaluateDocumentRanking as Super
from structure import step
from structure.plugin.pyspark import cross_join, group_by, inner_join, param_join, where


class EvaluateDocumentRanking(Super):
    """Evaluate rankings selected by both caller query labels and one user band."""

    @step(output=Super.evaluated_queries)
    def select_queries(
        self,
        query: SearchQuery,
        result: DocumentSearchResult,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> EvaluationQuery:
        """Apply the label predicate to the user-selected query/context population."""

        cross_join(batch, allow_cartesian=True)
        param_join(params)
        inner_join(on=result.search_query_id == query.id)
        where(
            params.matches_band(result.band_id),
            params.matches_query(query),
        )
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
