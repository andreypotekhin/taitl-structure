from structure.core.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.platform.pyspark import PySparkPlatform


def test_bundled_pyspark_platform_exposes_the_v1_facade() -> None:
    api = PySparkPlatform().api(1)

    assert PySparkPlatform.descriptor.name == "pyspark"
    assert api.schema is not None
    assert api.compiler is not None
    assert api.capabilities.resolve(profile=">=3.5,<4.1", variant="ordinary").require(
        CapabilityRequirement(group="join", name="inner_join")
    ).supported
    assert api.explainer is not None
