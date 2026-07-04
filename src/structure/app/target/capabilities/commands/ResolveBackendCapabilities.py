from structure.app.target.capabilities.logic.rules.PySparkCapabilityRules import (
    DEFAULT_TARGET_PROFILE,
    DEFAULT_TARGET_VARIANT,
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
        target_variant: str = DEFAULT_TARGET_VARIANT,
    ) -> BackendCapabilities:
        if target_backend == "pyspark":
            capabilities = PySparkCapabilities(target_profile=target_profile, target_variant=target_variant)
            capabilities.require(
                CapabilityRequirement(
                    group="backend",
                    name=capabilities.id.family,
                    docs="docs/reference/BackendCapabilities.md#pyspark-target-variants",
                )
            )
            return capabilities

        backend = BackendId(name=target_backend, target=target_profile, family="unknown", variant=target_variant)
        requirement = CapabilityRequirement(
            group="backend",
            name=target_backend,
            docs="docs/reference/BackendCapabilities.md#unsupported-backend-targets",
        )
        decision = CapabilityDecision.unsupported_backend(
            backend=backend,
            requirement=requirement,
            supported_backend="pyspark",
        )
        raise BackendCapabilityError(decision)
