from importlib.metadata import PackageNotFoundError, version

from structure.core.platform.EngineManifest import EngineManifest
from structure.core.platform.PlatformConfiguration import PlatformConfiguration

CORE_ENGINE_REVISION = "v1"


class ResolveEngine:
    def __call__(self, stock: type, *, platform: str, distribution: str, manifest: EngineManifest | None, configuration: PlatformConfiguration):
        if manifest is None or stock not in manifest.replacements:
            return stock
        if configuration.plugin_options != "allow_injection":
            raise ValueError(
                f"PLATFORM-E2708: Platform {platform!r} from {distribution!r} requests class injection; "
                "set platform.plugin_options = 'allow_injection' only for trusted plugins."
            )
        if manifest.core_engine_revision != CORE_ENGINE_REVISION:
            raise ValueError(f"PLATFORM-E2708: Platform {platform!r} requires engine revision {manifest.core_engine_revision!r}; current revision is {CORE_ENGINE_REVISION!r}.")
        if manifest.requires_structure != self._version():
            raise ValueError(f"PLATFORM-E2708: Platform {platform!r} requires Structure {manifest.requires_structure!r}; current version is {self._version()!r}.")
        return manifest.replacements[stock]

    def _version(self) -> str:
        try:
            return version("structure")
        except PackageNotFoundError:
            return "unknown"
