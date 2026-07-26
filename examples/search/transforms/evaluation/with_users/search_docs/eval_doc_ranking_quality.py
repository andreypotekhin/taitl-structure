"""User-band-sliced judged document-ranking evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import EvaluationQuery
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.user import CohortLineage, CohortMembership, UserBand
from examples.search.transforms.evaluation.search_docs import (
    EvaluateDocumentRankingQuality as BaseEvaluateDocumentRankingQuality,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, group_by, inner_join, where


class EvaluateDocumentRankingQuality(BaseEvaluateDocumentRankingQuality):
    """Evaluate context-specific rankings for users matching one persisted band."""

    requests = input(SearchRequest)
    memberships = input(CohortMembership)
    cohort_lineage = input(CohortLineage)
    user_bands = input(UserBand)
    params = input(EvaluationParams)

    @step(output=BaseEvaluateDocumentRankingQuality.evaluated_queries)
    def select_queries(
        self,
        query: SearchQuery,
        request: SearchRequest,
        membership: CohortMembership,
        lineage: CohortLineage,
        user_band: UserBand,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> EvaluationQuery:
        """Select one query/context population for the requested user band."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=request.query_id == query.id)
        inner_join(on=membership.user_id == request.user_id)
        inner_join(on=lineage.cohort_id == membership.cohort_id)
        inner_join(on=user_band.user_id == request.user_id)
        where(params.user_band.is_not_null() & (lineage.ancestor_cohort_id == params.user_band.id))
        group_by(
            window=batch.window,
            params=params,
            experiment_id="",
            band_id=user_band.band_id,
            search_query_id=query.id,
        )
        return EvaluationQuery(
            window=batch.window,
            params=EvaluationParams(labels=params.labels, user_band=params.user_band),
            experiment_id="",
            band_id=user_band.band_id,
            search_query_id=query.id,
        )
