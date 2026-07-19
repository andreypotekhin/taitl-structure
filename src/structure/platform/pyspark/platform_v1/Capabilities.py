from structure.platform.api.v1 import CapabilitiesAPI
from structure.platform.pyspark.capabilities.PySparkCapabilityRules import PySparkCapabilities


class Capabilities(CapabilitiesAPI):
    def resolve(self, *, profile: str, variant: str) -> PySparkCapabilities:
        return PySparkCapabilities(target_profile=profile, target_variant=variant)
