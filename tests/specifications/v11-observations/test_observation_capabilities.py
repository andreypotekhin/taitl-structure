import pytest

from structure.plugin.api.v1.model import CapabilityRequirement
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities


@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
def test_batch_observation_is_admitted_on_exact_41_profiles(variant):
    capabilities = PySparkCapabilities(target_profile=">=4.1,<4.2", target_variant=variant)

    assert capabilities.supports(CapabilityRequirement(group="observation", name="batch")).supported


@pytest.mark.parametrize(
    ("profile", "variant"),
    [
        (">=3.5,<4.1", "ordinary"),
        (">=4.0,<4.1", "ordinary"),
        (">=4.2,<4.3", "ordinary"),
        (">=4.2,<4.3", "spark-connect"),
    ],
)
def test_batch_observation_is_rejected_outside_exact_41_profiles(profile, variant):
    capabilities = PySparkCapabilities(target_profile=profile, target_variant=variant)
    decision = capabilities.supports(CapabilityRequirement(group="observation", name="batch"))

    assert not decision.supported
    assert decision.required_target == ">=4.1,<4.2"
