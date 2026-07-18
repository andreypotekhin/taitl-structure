import pytest

from structure import Transform, transform
from structure.core.platform import PlatformConfiguration, ResolvePlatformTarget


@transform(target="fake")
class FakeTransform(Transform):
    pass


@transform(target="other")
class OtherTransform(Transform):
    pass


class DefaultTransform(Transform):
    pass


def test_platform_tables_merge_and_list_values_replace() -> None:
    configuration = PlatformConfiguration.resolve(
        {"platform": {"disabled_distributions": ["old"], "fake": {"profile": "v1", "variant": "a"}}},
        {"platform": {"disabled_distributions": ["new"], "fake": {"profile": "v2"}}},
    )

    assert configuration.disabled_distributions == frozenset({"new"})
    assert dict(configuration.plugins["fake"]) == {"profile": "v2", "variant": "a"}


def test_target_resolution_prefers_declaration_then_explicit_then_default() -> None:
    resolver = ResolvePlatformTarget()
    configuration = PlatformConfiguration.resolve({"platform": {"default": "default"}})

    assert resolver(FakeTransform, configuration=configuration) == "fake"
    assert resolver(DefaultTransform, configuration=configuration, target="explicit") == "explicit"
    assert resolver(DefaultTransform, configuration=configuration) == "default"


def test_target_resolution_rejects_conflicts_missing_targets_and_cross_target_pipelines() -> None:
    resolver = ResolvePlatformTarget()
    empty = PlatformConfiguration.resolve({})

    with pytest.raises(ValueError, match="PLATFORM-E2703"):
        resolver(FakeTransform, configuration=empty, target="other")
    with pytest.raises(ValueError, match="PLATFORM-E2702"):
        resolver(DefaultTransform, configuration=empty)
    with pytest.raises(ValueError, match="PLATFORM-E2711"):
        resolver(FakeTransform().to(OtherTransform()), configuration=empty)
