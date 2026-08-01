from examples.store.schemas.experiment import RecommendationAssignment, RecommendationExperiment
from examples.store.schemas.merchandising import RecommendationRequest
from structure import Transform, input, output
from structure.plugin.pyspark import abs, coalesce, inner_join, when, xxhash64


class AssignRecommendationVariants(Transform):
    """Assign a stable request key to one active experiment variant."""

    requests = input(RecommendationRequest)
    experiments = input(RecommendationExperiment)
    assignments = output(RecommendationAssignment)

    def assign(self, request: RecommendationRequest, experiment: RecommendationExperiment) -> RecommendationAssignment:
        inner_join(
            experiment,
            on=(experiment.tenant.tenant_id == request.tenant.tenant_id) & experiment.active,
        )
        assignment_key = coalesce(request.customer_id, request.session_id, request.id)
        bucket = abs(xxhash64(experiment.experiment_id, experiment.experiment_version, assignment_key)) % 100
        return RecommendationAssignment.project(experiment)(
            assignment_key=assignment_key,
            variant_id=(
                when(bucket < experiment.variant_a_percent, experiment.variant_a).otherwise(experiment.variant_b)
            ),
            assigned_at=request.requested_at,
        )
