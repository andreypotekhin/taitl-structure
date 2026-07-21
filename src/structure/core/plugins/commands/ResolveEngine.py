from structure.core.plugins.model.EngineManifest import EngineManifest
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.version import VERSION

CORE_ENGINE_REVISION = "v1"


class ResolveEngine:
    def __call__(self, stock: type, *, plugin: str, distribution: str, manifest: EngineManifest | None, configuration: PluginConfiguration):
        if manifest is None or stock not in manifest.replacements:
            return stock
        if configuration.plugin_options != "allow_injection":
            raise ValueError(
                f"PLUGIN-E2708: Plugin {plugin!r} from {distribution!r} requests class injection; "
                "set plugin.plugin_options = 'allow_injection' only for trusted plugins."
            )
        if manifest.core_engine_revision != CORE_ENGINE_REVISION:
            raise ValueError(f"PLUGIN-E2708: Plugin {plugin!r} requires engine revision {manifest.core_engine_revision!r}; current revision is {CORE_ENGINE_REVISION!r}.")
        if manifest.requires_structure != self._version():
            raise ValueError(f"PLUGIN-E2708: Plugin {plugin!r} requires Structure {manifest.requires_structure!r}; current version is {self._version()!r}.")
        return manifest.replacements[stock]

    def _version(self) -> str:
        return VERSION
