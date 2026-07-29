"""Combined label-and-user-band observed document-search behavior evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.behavior import BehaviorRequest
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.user import BandMembership
from examples.search.transforms.evaluation.search_docs.behavior.with_users import (
    EvaluateDocumentSearchBehavior as Super,
)
from structure import input, step
from structure.plugin.pyspark import cross_join, inner_join, where


class EvaluateDocumentSearchBehavior(Super):
    """Measure served behavior selected by both labels and one user band."""

    queries = input(SearchQuery)

    @step(output=Super.selected_requests)
    def select_requests(
        self,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> BehaviorRequest:
        """Apply the query-label predicate after user-band request selection."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=query.id == request.query_id)
        inner_join(on=band.user_id == request.user_id)
        where(
            params.matches_band(band.band_id),
            params.matches_query(query),
            (request.requested_at >= batch.window.start) & (request.requested_at < batch.window.end),
        )
        return BehaviorRequest(
            window=batch.window,
            params=EvaluationParams(queryset=params.queryset, labels=params.labels, band_id=params.band_id),
            experiment_id=request.experiment_id,
            band_id=band.user_band_id,
            search_request_id=request.id,
            ranking_version=request.ranking_version,
            query=request.query,
        )
