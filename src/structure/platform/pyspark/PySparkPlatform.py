from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.platform.api import PlatformDescriptor
from structure.version import VERSION

if TYPE_CHECKING:
    from structure.platform.api.v1 import PlatformAPIV1


@dataclass(frozen=True)
class PySparkPlatform:
    descriptor = PlatformDescriptor("pyspark", "PySpark", "structure", VERSION, 1, 1)

    @classmethod
    def api(cls, version: int) -> PlatformAPIV1:
        if version != 1:
            raise ValueError(f"PySpark does not support Platform API v{version}.")
        from structure.platform.pyspark.api.PlatformAPI import PlatformAPI

        return PlatformAPI().create()
