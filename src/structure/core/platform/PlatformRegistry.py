import re
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
    _name = re.compile(r"[a-z][a-z0-9-]*$")

    def __init__(self, entries: Callable[[], Iterable[Any]] | None = None) -> None:
        self._entries = entries or self._installed_entries

    def discover(self, *, disabled_distributions: frozenset[str] = frozenset()) -> tuple[DiscoveredPlatform, ...]:
        disabled = {self._distribution(name) for name in disabled_distributions}
        platforms = tuple(
            self._platform(item)
            for item in self._entries()
            if item.group == PLATFORM_ENTRY_POINT_GROUP
            and (item.dist is None or self._distribution(item.dist.name) not in disabled)
        )
        names = {platform.name for platform in platforms}
        for name in names:
            matches = [platform.distribution for platform in platforms if platform.name == name]
            if len(matches) > 1:
                raise ValueError(
                    f"PLATFORM-E2704: Platform {name!r} is supplied by multiple distributions: "
                    f"{', '.join(matches)}. Disable one with platform.disabled_distributions."
                )
        return platforms

    def select(self, name: str, *, disabled_distributions: frozenset[str] = frozenset()) -> SelectedPlatform:
        matches = [item for item in self.discover(disabled_distributions=disabled_distributions) if item.name == name]
        if not matches:
            raise ValueError(f"PLATFORM-E2702: Platform {name!r} is not installed or is disabled.")
        discovered = matches[0]
        try:
            plugin = discovered.load()
        except Exception as error:
            raise ValueError(
                f"PLATFORM-E2705: Could not load platform {name!r} from distribution "
                f"{discovered.distribution!r}: {type(error).__name__}: {error}"
            ) from error
        descriptor = plugin.descriptor
        if descriptor.name != name:
            raise ValueError(
                f"PLATFORM-E2706: Platform entry point {name!r} loaded a plugin named {descriptor.name!r}."
            )
        if self._distribution(descriptor.distribution) != self._distribution(discovered.distribution):
            raise ValueError(
                f"PLATFORM-E2706: Platform {name!r} declares distribution {descriptor.distribution!r}, "
                f"but was discovered from {discovered.distribution!r}."
            )
        version = min(CORE_API_MAX_VERSION, descriptor.maximum_api_version)
        if version < max(CORE_API_MIN_VERSION, descriptor.minimum_api_version):
            raise ValueError(f"PLATFORM-E2707: Platform {name!r} has no compatible Platform API version.")
        try:
            api = plugin.api(version)
        except Exception as error:
            raise ValueError(
                f"PLATFORM-E2708: Platform {name!r} could not provide its advertised Platform API v{version}: "
                f"{type(error).__name__}: {error}"
            ) from error
        if not isinstance(api, PlatformAPI) or api.schema is None or api.compiler is None or api.capabilities is None:
            raise ValueError(f"PLATFORM-E2708: Platform {name!r} did not supply a complete Platform API v{version} façade.")
        return SelectedPlatform(descriptor=descriptor, api_version=version, api=api)

    def _installed_entries(self) -> Iterable[Any]:
        return entry_points(group=PLATFORM_ENTRY_POINT_GROUP)

    def _platform(self, item: Any) -> DiscoveredPlatform:
        if self._name.fullmatch(item.name) is None:
            raise ValueError(f"PLATFORM-E2706: Platform entry-point name {item.name!r} is not a lowercase identifier.")
        return DiscoveredPlatform(name=item.name, distribution=item.dist.name if item.dist else "unknown", load=item.load)

    def _distribution(self, name: str) -> str:
        return re.sub(r"[-_.]+", "-", name).lower()
