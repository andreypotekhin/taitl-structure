from typing import Any, Callable, Iterable

from structure.core.plugins.commands.ResolveEngine import CORE_ENGINE_REVISION, ResolveEngine
from structure.core.plugins.commands.ResolvePluginTarget import ResolvePluginTarget
from structure.core.plugins.logic.PluginRegistry import PluginRegistry


class Plugin:

    @staticmethod
    def registry(
        entries: Callable[[], Iterable[Any]] | None = None,
        *,
        minimum_api_version: int | None = None,
        maximum_api_version: int | None = None,
    ) -> PluginRegistry:
        options = {}
        if minimum_api_version is not None:
            options["minimum_api_version"] = minimum_api_version
        if maximum_api_version is not None:
            options["maximum_api_version"] = maximum_api_version
        return PluginRegistry(entries, **options)

    @staticmethod
    def resolve_engine() -> ResolveEngine:
        return ResolveEngine()

    @staticmethod
    def resolve_target() -> ResolvePluginTarget:
        return ResolvePluginTarget()

    @staticmethod
    def engine_revision() -> str:
        return CORE_ENGINE_REVISION
