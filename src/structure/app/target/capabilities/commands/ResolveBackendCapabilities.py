from structure.app.target.capabilities.logic.rules.PySparkCapabilityRules import (
    DEFAULT_TARGET_PROFILE,
    PySparkCapabilities,
)
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.model.BackendCapabilityError import BackendCapabilityError
from structure.app.target.capabilities.model.BackendId import BackendId
from structure.app.target.capabilities.model.CapabilityDecision import CapabilityDecision
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement


class ResolveBackendCapabilities:

    def __call__(
        self,
        *,
        target_backend: str = "pyspark",
        target_profile: str = DEFAULT_TARGET_PROFILE,
    ) -> BackendCapabilities:
        if target_backend == "pyspark":
            return PySparkCapabilities(target_profile=target_profile)

        backend = BackendId(name=target_backend, target=target_profile, family="unknown")
        requirement = CapabilityRequirement(
            group="backend",
            name=target_backend,
            docs="docs/specifications/BackendCapabilities.md#unsupported-backend-targets",
        )
        decision = CapabilityDecision.unsupported_backend(
            backend=backend,
            requirement=requirement,
            supported_backend="pyspark",
        )
        raise BackendCapabilityError(decision)
