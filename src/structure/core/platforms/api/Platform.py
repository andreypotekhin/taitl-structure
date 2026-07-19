from typing import Any, Callable, Iterable

from structure.core.platforms.commands.ResolveEngine import CORE_ENGINE_REVISION, ResolveEngine
from structure.core.platforms.commands.ResolvePlatformTarget import ResolvePlatformTarget
from structure.core.platforms.logic.PlatformRegistry import PlatformRegistry


class Platform:

    @staticmethod
    def registry(
        entries: Callable[[], Iterable[Any]] | None = None,
        *,
        minimum_api_version: int | None = None,
        maximum_api_version: int | None = None,
    ) -> PlatformRegistry:
        options = {}
        if minimum_api_version is not None:
            options["minimum_api_version"] = minimum_api_version
        if maximum_api_version is not None:
            options["maximum_api_version"] = maximum_api_version
        return PlatformRegistry(entries, **options)

    @staticmethod
    def resolve_engine() -> ResolveEngine:
        return ResolveEngine()

    @staticmethod
    def resolve_target() -> ResolvePlatformTarget:
        return ResolvePlatformTarget()

    @staticmethod
    def engine_revision() -> str:
        return CORE_ENGINE_REVISION
