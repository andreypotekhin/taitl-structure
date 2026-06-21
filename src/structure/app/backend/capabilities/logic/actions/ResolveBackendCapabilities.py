from structure.app.backend.capabilities.logic.model.BackendCapabilities import BackendCapabilities
from structure.app.backend.capabilities.logic.model.BackendCapabilityError import BackendCapabilityError
from structure.app.backend.capabilities.logic.model.BackendId import BackendId
from structure.app.backend.capabilities.logic.model.CapabilityDecision import CapabilityDecision
from structure.app.backend.capabilities.logic.model.CapabilityRequirement import CapabilityRequirement
from structure.app.backend.capabilities.logic.rules.PySparkCapabilityRules import (
    DEFAULT_TARGET_PYSPARK,
    PySparkCapabilities,
)


class ResolveBackendCapabilities:

    def __call__(
        self,
        *,
        target_backend: str = "pyspark",
        target_pyspark: str = DEFAULT_TARGET_PYSPARK,
    ) -> BackendCapabilities:
        if target_backend == "pyspark":
            return PySparkCapabilities(target_pyspark=target_pyspark)

        backend = BackendId(name=target_backend, target=target_pyspark, family="unknown")
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
