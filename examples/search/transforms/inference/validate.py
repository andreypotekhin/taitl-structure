"""Validate inference policy identity and vector shape."""

from examples.search.schemas.inference import InferencePolicy
from structure import Transform, input, output, step
from structure.plugin.pyspark import require_all


class ValidateInferencePolicy(Transform):
    """Reject malformed provider identity or vector dimensions before inference."""

    policy = input(InferencePolicy)
    valid_policy = output(InferencePolicy)

    @step(input=policy, output=valid_policy)
    def validate(self, policy: InferencePolicy) -> InferencePolicy:
        require_all(
            (policy.provider_id != "")
            & (policy.model_id != "")
            & (policy.model_version != "")
            & (policy.content_revision != "")
            & (policy.dimension > 0)
            & (policy.experiment_id != "")
        )
        return InferencePolicy.project(policy)
