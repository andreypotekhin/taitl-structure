from structure.app.target.capabilities.logic.rules.PySparkCapabilityRules import (
    DEFAULT_TARGET_PYSPARK,
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
