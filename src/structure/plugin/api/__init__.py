from dataclasses import dataclass
from typing import Protocol


PLUGIN_ENTRY_POINT_GROUP = "structure.plugin"
CORE_API_MIN_VERSION = 1
CORE_API_MAX_VERSION = 1


@dataclass(frozen=True)
class PluginDescriptor:
    name: str
    display_name: str
    distribution: str
    plugin_version: str
    minimum_api_version: int
    maximum_api_version: int


class Plugin(Protocol):
    @property
    def descriptor(self) -> PluginDescriptor: ...

    def api(self, version: int): ...
