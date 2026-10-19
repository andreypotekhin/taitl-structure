from types import SimpleNamespace

import pytest

from structure.plugin.api.v1.model import CapabilityRequirement
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.commands.ClassifyStreamingCompatibility import ClassifyStreamingCompatibility
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@pytest.mark.parametrize("operation", ["correlation", "exists", "in_subquery", "lateral_join"])
@pytest.mark.parametrize(
    ("profile", "variant"),
    [
        (">=3.5,<4.1", "ordinary"),
        (">=4.0,<4.1", "ordinary"),
        (">=4.2,<4.3", "ordinary"),
        (">=4.2,<4.3", "spark-connect"),
    ],
)
def test_query_operations_remain_gated_on_earlier_profiles(operation, profile, variant) -> None:
    capabilities = PySparkCapabilities(target_profile=profile, target_variant=variant)
    decision = capabilities.supports(CapabilityRequirement(group="query", name=operation))

    assert not decision.supported
    assert decision.required_target == ">=4.1,<4.2"


@pytest.mark.parametrize("operation", ["correlation", "exists", "in_subquery", "lateral_join"])
@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
def test_query_operations_are_admitted_on_exact_41_profiles(operation, variant) -> None:
    capabilities = PySparkCapabilities(target_profile=">=4.1,<4.2", target_variant=variant)
    decision = capabilities.supports(CapabilityRequirement(group="query", name=operation))

    assert decision.supported


def test_query_expressions_are_classified_as_batch_only_for_streaming_steps() -> None:
    expression = PySparkExpressionRecipe(kind="query_exists", type=None, nullable=False, data={}, args=())
    step = SimpleNamespace(joins=(), filters=(expression,), projection=(), results=())

    assert ClassifyStreamingCompatibility._uses_typed_query(step)
