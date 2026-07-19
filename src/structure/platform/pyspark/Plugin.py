from dataclasses import dataclass

from structure.platform.api import PlatformDescriptor
from structure.platform.api.v1 import PlatformAPI as V1PlatformAPI
from structure.platform.pyspark.platform_v1.PlatformAPI import PlatformAPI
from structure.version import VERSION


@dataclass(frozen=True)
class PySparkPlatform:
    descriptor = PlatformDescriptor("pyspark", "PySpark", "structure", VERSION, 1, 1)

    @classmethod
    def api(cls, version: int) -> V1PlatformAPI:
        if version != 1:
            raise ValueError(f"PySpark does not support Platform API v{version}.")
        return PlatformAPI().create()
