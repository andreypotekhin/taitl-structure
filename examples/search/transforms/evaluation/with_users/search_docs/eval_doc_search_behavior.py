"""User-band-sliced observed document-search behavior evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.behavior import BehaviorRequest
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.user import CohortMembership, UserBand
from examples.search.transforms.evaluation.search_docs.eval_doc_search_behavior import (
    EvaluateDocumentSearchBehavior as BaseEvaluateDocumentSearchBehavior,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, inner_join, where


class EvaluateDocumentSearchBehavior(BaseEvaluateDocumentSearchBehavior):
    """Measure served behavior for requests whose users match one persisted band."""

    memberships = input(CohortMembership)
    user_bands = input(UserBand)
    params = input(EvaluationParams)

    @step(output=BaseEvaluateDocumentSearchBehavior.selected_requests)
    def select_requests(
        self,
        request: SearchRequest,
        membership: CohortMembership,
        user_band: UserBand,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> BehaviorRequest:
        """Keep batch-window requests for users matching the requested user band."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=membership.user_id == request.user_id)
        inner_join(on=user_band.user_id == request.user_id)
        where(
            params.user_band.is_not_null(),
            membership.cohort_id == params.user_band.id,
            (request.requested_at >= batch.window.start) & (request.requested_at < batch.window.end),
        )
        return BehaviorRequest(
            window=batch.window,
            params=EvaluationParams(labels=params.labels, user_band=params.user_band),
            experiment_id=request.experiment_id,
            band_id=user_band.band_id,
            search_request_id=request.id,
            ranking_version=request.ranking_version,
            query=request.query,
        )
