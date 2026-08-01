from examples.store.schemas.experiment import RecommendationAssignment, RecommendationExposure
from examples.store.schemas.merchandising import RecommendationRequest, RecommendationRun
from structure import Transform, input, output
from structure.plugin.pyspark import coalesce, inner_join, where


class RecordRecommendationExposures(Transform):
    """Record only requests that actually served at least one recommendation."""

    requests = input(RecommendationRequest)
    assignments = input(RecommendationAssignment)
    runs = input(RecommendationRun)
    exposures = output(RecommendationExposure)

    def record(
        self, request: RecommendationRequest, assignment: RecommendationAssignment, run: RecommendationRun
    ) -> RecommendationExposure:
        inner_join(
            assignment,
            on=(assignment.tenant.tenant_id == request.tenant.tenant_id)
            & (assignment.assignment_key == coalesce(request.customer_id, request.session_id, request.id)),
        )
        inner_join(
            run,
            on=(run.tenant.tenant_id == request.tenant.tenant_id) & (run.request_id == request.id),
        )
        return RecommendationExposure.project(assignment)(
            request_id=request.id,
            exposed_at=request.requested_at,
        )
