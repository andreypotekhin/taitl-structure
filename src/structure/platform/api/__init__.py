from dataclasses import dataclass
from typing import Protocol


PLATFORM_ENTRY_POINT_GROUP = "structure.platform"
CORE_API_MIN_VERSION = 1
CORE_API_MAX_VERSION = 1


@dataclass(frozen=True)
class PlatformDescriptor:
    name: str
    display_name: str
    distribution: str
    plugin_version: str
    minimum_api_version: int
    maximum_api_version: int


class PlatformPlugin(Protocol):
    @property
    def descriptor(self) -> PlatformDescriptor: ...

    def api(self, version: int): ...
