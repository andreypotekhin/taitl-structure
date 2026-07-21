from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities


class ResolvePySparkCapabilities:

    def __call__(self, *, profile: str = "", variant: str = "") -> PySparkCapabilities:
        return PySparkCapabilities(target_profile=profile, target_variant=variant)
