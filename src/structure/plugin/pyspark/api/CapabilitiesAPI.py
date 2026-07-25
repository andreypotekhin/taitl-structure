from collections.abc import Mapping

from structure.plugin.api.v1 import CapabilitiesAPI as CapabilitiesAPIV1
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import (
    DEFAULT_TARGET_PROFILE,
    DEFAULT_TARGET_VARIANT,
)
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities as ResolvedCapabilities


class CapabilitiesAPI(CapabilitiesAPIV1):
    def resolve(self, *, options: Mapping[str, object]) -> ResolvedCapabilities:
        return PySpark.capabilities.resolve()(
            profile=str(options.get("profile", DEFAULT_TARGET_PROFILE)),
            variant=str(options.get("variant", DEFAULT_TARGET_VARIANT)),
        )
