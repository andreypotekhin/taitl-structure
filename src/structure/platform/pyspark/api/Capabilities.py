from structure.platform.api.v1 import CapabilitiesAPI
from structure.platform.pyspark.api.PySpark import PySpark
from structure.platform.pyspark.capabilities.model.PySparkCapabilities import (
    PySparkCapabilities as ResolvedCapabilities,
)


class Capabilities(CapabilitiesAPI):
    def resolve(self, *, profile: str, variant: str) -> ResolvedCapabilities:
        return PySpark.capabilities.resolve()(profile=profile, variant=variant)
