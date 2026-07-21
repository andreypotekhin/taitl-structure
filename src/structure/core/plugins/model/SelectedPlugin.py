from dataclasses import dataclass

from structure.plugin.api import PluginDescriptor
from structure.plugin.api.v1 import PluginAPI


@dataclass(frozen=True)
class SelectedPlugin:
    descriptor: PluginDescriptor
    api_version: int
    api: PluginAPI
