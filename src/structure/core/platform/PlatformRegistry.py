from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any, Callable, Iterable

from structure.platform.api import (
    CORE_API_MAX_VERSION,
    CORE_API_MIN_VERSION,
    PLATFORM_ENTRY_POINT_GROUP,
    PlatformDescriptor,
    PlatformPlugin,
)
from structure.platform.api.v1 import PlatformAPI


@dataclass(frozen=True)
class DiscoveredPlatform:
    name: str
    distribution: str
    load: Callable[[], PlatformPlugin]


@dataclass(frozen=True)
class SelectedPlatform:
    descriptor: PlatformDescriptor
    api_version: int
    api: PlatformAPI


class PlatformRegistry:
    def __init__(self, entries: Callable[[], Iterable[Any]] | None = None) -> None:
        self._entries = entries or self._installed_entries

    def discover(self, *, disabled_distributions: frozenset[str] = frozenset()) -> tuple[DiscoveredPlatform, ...]:
        platforms = tuple(
            DiscoveredPlatform(name=item.name, distribution=item.dist.name if item.dist else "unknown", load=item.load)
            for item in self._entries()
            if item.group == PLATFORM_ENTRY_POINT_GROUP
            and (item.dist is None or item.dist.name not in disabled_distributions)
        )
        names = {platform.name for platform in platforms}
        for name in names:
            matches = [platform.distribution for platform in platforms if platform.name == name]
            if len(matches) > 1:
                raise ValueError(f"Platform {name!r} is supplied by multiple distributions: {', '.join(matches)}.")
        return platforms

    def select(self, name: str, *, disabled_distributions: frozenset[str] = frozenset()) -> SelectedPlatform:
        matches = [item for item in self.discover(disabled_distributions=disabled_distributions) if item.name == name]
        if not matches:
            raise ValueError(f"Platform {name!r} is not installed or is disabled.")
        plugin = matches[0].load()
        descriptor = plugin.descriptor
        if descriptor.name != name:
            raise ValueError(f"Platform entry point {name!r} loaded a plugin named {descriptor.name!r}.")
        version = min(CORE_API_MAX_VERSION, descriptor.maximum_api_version)
        if version < max(CORE_API_MIN_VERSION, descriptor.minimum_api_version):
            raise ValueError(f"Platform {name!r} has no compatible Platform API version.")
        api = plugin.api(version)
        if not isinstance(api, PlatformAPI) or api.schema is None or api.compiler is None or api.capabilities is None:
            raise ValueError(f"Platform {name!r} did not supply a complete Platform API v{version} façade.")
        return SelectedPlatform(descriptor=descriptor, api_version=version, api=api)

    def _installed_entries(self) -> Iterable[Any]:
        return entry_points(group=PLATFORM_ENTRY_POINT_GROUP)
