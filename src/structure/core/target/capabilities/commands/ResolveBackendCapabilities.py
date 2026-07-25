from collections.abc import Mapping
from typing import cast

from structure.core.plugins.api.Plugin import Plugin
from structure.core.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.core.target.capabilities.model.BackendCapabilityError import BackendCapabilityError
from structure.core.target.capabilities.model.BackendId import BackendId
from structure.core.target.capabilities.model.CapabilityDecision import CapabilityDecision
from structure.core.target.capabilities.model.CapabilityRequirement import CapabilityRequirement


class ResolveBackendCapabilities:

    def __call__(
        self,
        *,
        target: str | None = None,
        options: Mapping[str, object] | None = None,
    ) -> BackendCapabilities:
        target_name = target or "pyspark"
        try:
            plugin = Plugin.registry().select(target_name)
        except ValueError:
            return self._unsupported(target_name, options or {}, self._available_targets())
        capabilities = cast(BackendCapabilities, plugin.api.capabilities.resolve(options=options or {}))
        require = getattr(capabilities, "require", None)
        identifier = getattr(capabilities, "id", None)
        if callable(require) and identifier is not None:
            require(
                CapabilityRequirement(
                    group="backend",
                    name=identifier.family,
                    docs="docs/reference/BackendCapabilities.md#pyspark-target-variants",
                )
            )
        return capabilities

    def _unsupported(
        self, target_name: str, options: Mapping[str, object], available_targets: tuple[str, ...]
    ) -> BackendCapabilities:
        backend = BackendId(
            name=target_name,
            target=str(options.get("profile", "")),
            family="unknown",
            variant=str(options.get("variant", "")),
        )
        requirement = CapabilityRequirement(
            group="backend",
            name=target_name,
            docs="docs/reference/BackendCapabilities.md#unsupported-backend-targets",
        )
        decision = CapabilityDecision.unsupported_backend(
            backend=backend,
            requirement=requirement,
            available_targets=available_targets,
        )
        raise BackendCapabilityError(decision)

    def _available_targets(self) -> tuple[str, ...]:
        return tuple(plugin.name for plugin in Plugin.registry().discover())
