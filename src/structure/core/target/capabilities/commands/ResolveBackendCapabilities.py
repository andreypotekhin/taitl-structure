from typing import cast

from structure.core.platforms.api.Platform import Platform
from structure.core.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.core.target.capabilities.model.BackendCapabilityError import BackendCapabilityError
from structure.core.target.capabilities.model.BackendId import BackendId
from structure.core.target.capabilities.model.CapabilityDecision import CapabilityDecision
from structure.core.target.capabilities.model.CapabilityRequirement import CapabilityRequirement

DEFAULT_TARGET_PROFILE = ">=3.5,<4.1"
DEFAULT_TARGET_VARIANT = "ordinary"


class ResolveBackendCapabilities:

    def __call__(
        self,
        *,
        target_backend: str = "pyspark",
        target_profile: str = DEFAULT_TARGET_PROFILE,
        target_variant: str = DEFAULT_TARGET_VARIANT,
    ) -> BackendCapabilities:
        try:
            platform = Platform.registry().select(target_backend)
        except ValueError:
            return self._unsupported(target_backend, target_profile, target_variant)
        capabilities = cast(BackendCapabilities, platform.api.capabilities.resolve(profile=target_profile, variant=target_variant))
        capabilities.require(
            CapabilityRequirement(
                group="backend",
                name=capabilities.id.family,
                docs="docs/reference/BackendCapabilities.md#pyspark-target-variants",
            )
        )
        return capabilities

    def _unsupported(self, target_backend: str, target_profile: str, target_variant: str) -> BackendCapabilities:
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
