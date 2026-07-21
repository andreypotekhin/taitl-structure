import re
from importlib.metadata import entry_points
from typing import Any, Callable, Iterable

from structure.core.plugins.model.DiscoveredPlugin import DiscoveredPlugin
from structure.core.plugins.model.SelectedPlugin import SelectedPlugin
from structure.plugin.api import CORE_API_MAX_VERSION, CORE_API_MIN_VERSION, PLUGIN_ENTRY_POINT_GROUP
from structure.plugin.api.v1 import PluginAPI
from structure.plugin.BundledPySparkEntry import BundledPySparkEntry


class PluginRegistry:
    _name = re.compile(r"[a-z][a-z0-9-]*$")

    def __init__(
        self,
        entries: Callable[[], Iterable[Any]] | None = None,
        *,
        minimum_api_version: int = CORE_API_MIN_VERSION,
        maximum_api_version: int = CORE_API_MAX_VERSION,
    ) -> None:
        self._entries = entries or self._installed_entries
        self._minimum_api_version = minimum_api_version
        self._maximum_api_version = maximum_api_version

    def discover(self, *, disabled_distributions: frozenset[str] = frozenset()) -> tuple[DiscoveredPlugin, ...]:
        disabled = {self._distribution(name) for name in disabled_distributions}
        plugins = tuple(
            self._plugin(item)
            for item in self._entries()
            if item.group == PLUGIN_ENTRY_POINT_GROUP
            and (item.dist is None or self._distribution(item.dist.name) not in disabled)
        )
        names = {plugin.name for plugin in plugins}
        for name in names:
            matches = [plugin.distribution for plugin in plugins if plugin.name == name]
            if len(matches) > 1:
                raise ValueError(
                    f"PLUGIN-E2704: Plugin {name!r} is supplied by multiple distributions: "
                    f"{', '.join(matches)}. Disable one with plugin.disabled_distributions."
                )
        return plugins

    def select(self, name: str, *, disabled_distributions: frozenset[str] = frozenset()) -> SelectedPlugin:
        matches = [item for item in self.discover(disabled_distributions=disabled_distributions) if item.name == name]
        if not matches:
            raise ValueError(f"PLUGIN-E2702: Plugin {name!r} is not installed or is disabled.")
        discovered = matches[0]
        try:
            plugin = discovered.load()
        except Exception as error:
            raise ValueError(
                f"PLUGIN-E2705: Could not load plugin {name!r} from distribution "
                f"{discovered.distribution!r}: {type(error).__name__}: {error}"
            ) from error
        descriptor = plugin.descriptor
        if descriptor.name != name:
            raise ValueError(
                f"PLUGIN-E2706: Plugin entry point {name!r} loaded a plugin named {descriptor.name!r}."
            )
        if self._distribution(descriptor.distribution) != self._distribution(discovered.distribution):
            raise ValueError(
                f"PLUGIN-E2706: Plugin {name!r} declares distribution {descriptor.distribution!r}, "
                f"but was discovered from {discovered.distribution!r}."
            )
        version = min(self._maximum_api_version, descriptor.maximum_api_version)
        if version < max(self._minimum_api_version, descriptor.minimum_api_version):
            raise ValueError(f"PLUGIN-E2707: Plugin {name!r} has no compatible Plugin API version.")
        try:
            api = plugin.api(version)
        except Exception as error:
            raise ValueError(
                f"PLUGIN-E2708: Plugin {name!r} could not provide its advertised Plugin API v{version}: "
                f"{type(error).__name__}: {error}"
            ) from error
        if not isinstance(api, PluginAPI) or api.schema is None or api.compiler is None or api.capabilities is None:
            raise ValueError(f"PLUGIN-E2708: Plugin {name!r} did not supply a complete Plugin API v{version} façade.")
        return SelectedPlugin(descriptor=descriptor, api_version=version, api=api)

    def _installed_entries(self) -> Iterable[Any]:
        installed = tuple(entry_points(group=PLUGIN_ENTRY_POINT_GROUP))
        bundled = any(
            item.name == "pyspark"
            and item.dist is not None
            and self._distribution(getattr(item.dist, "name", None)) == "structure"
            for item in installed
        )
        return installed if bundled else (*installed, BundledPySparkEntry())

    def _plugin(self, item: Any) -> DiscoveredPlugin:
        if self._name.fullmatch(item.name) is None:
            raise ValueError(f"PLUGIN-E2706: Plugin entry-point name {item.name!r} is not a lowercase identifier.")
        distribution = getattr(item.dist, "name", None) if item.dist else None
        return DiscoveredPlugin(name=item.name, distribution=distribution or "unknown", load=item.load)

    def _distribution(self, name: str | None) -> str:
        if not name:
            return "unknown"
        return re.sub(r"[-_.]+", "-", name).lower()
