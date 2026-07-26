"""User-band-sliced observed document-search behavior evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.behavior import BehaviorRequest
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.user import BandMembership
from examples.search.transforms.evaluation.search_docs.eval_doc_search_behavior import (
    EvaluateDocumentSearchBehavior as Super,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, inner_join, where


class EvaluateDocumentSearchBehavior(Super):
    """Measure served behavior for requests whose users match one persisted band."""

    band_memberships = input(BandMembership)
    params = input(EvaluationParams)

    @step(output=Super.selected_requests)
    def select_requests(
        self,
        request: SearchRequest,
        band: BandMembership,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> BehaviorRequest:
        """Roll up observed requests through their materialized band policies."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=band.user_id == request.user_id)
        where(
            params.matches_band(band.band_id),
            (request.requested_at >= batch.window.start) & (request.requested_at < batch.window.end),
        )
        return BehaviorRequest(
            window=batch.window,
            params=EvaluationParams(labels=params.labels, band_id=params.band_id),
            experiment_id=request.experiment_id,
            band_id=band.user_band_id,
            search_request_id=request.id,
            ranking_version=request.ranking_version,
            query=request.query,
        )
