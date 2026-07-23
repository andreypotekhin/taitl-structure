from structure.plugin.api.v1 import CapabilitiesAPI as CapabilitiesAPIV1
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities as ResolvedCapabilities


class CapabilitiesAPI(CapabilitiesAPIV1):
    def resolve(self, *, profile: str, variant: str) -> ResolvedCapabilities:
        return PySpark.capabilities.resolve()(profile=profile, variant=variant)
