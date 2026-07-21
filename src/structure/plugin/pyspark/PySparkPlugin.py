from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.plugin.api import PluginDescriptor
from structure.version import VERSION

if TYPE_CHECKING:
    from structure.plugin.api.v1 import PluginAPIV1


@dataclass(frozen=True)
class PySparkPlugin:
    descriptor = PluginDescriptor("pyspark", "PySpark", "structure", VERSION, 1, 1)

    @classmethod
    def api(cls, version: int) -> PluginAPIV1:
        if version != 1:
            raise ValueError(f"PySpark does not support Plugin API v{version}.")
        from structure.plugin.pyspark.api.PluginAPI import PluginAPI

        return PluginAPI().create()
