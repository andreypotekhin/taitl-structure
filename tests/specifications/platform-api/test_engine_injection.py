import pytest

from structure.core.platform import EngineManifest, PlatformConfiguration, ResolveEngine
from structure.core.platform.ResolveEngine import CORE_ENGINE_REVISION


class Stock:
    pass


class Replacement:
    pass


def test_engine_replacement_requires_an_explicit_global_opt_in() -> None:
    manifest = EngineManifest("0.0.4", CORE_ENGINE_REVISION, {Stock: Replacement})
    resolver = ResolveEngine()

    with pytest.raises(ValueError, match="allow_injection"):
        resolver(Stock, platform="fake", distribution="fake-wheel", manifest=manifest, configuration=PlatformConfiguration.resolve({}))

    enabled = PlatformConfiguration.resolve({"platform": {"plugin_options": "allow_injection"}})
    assert resolver(Stock, platform="fake", distribution="fake-wheel", manifest=manifest, configuration=enabled) is Replacement


def test_incompatible_manifest_never_falls_back_to_the_stock_engine() -> None:
    manifest = EngineManifest("0.0.4", "old", {Stock: Replacement})
    configuration = PlatformConfiguration.resolve({"platform": {"plugin_options": "allow_injection"}})

    with pytest.raises(ValueError, match="engine revision"):
        ResolveEngine()(Stock, platform="fake", distribution="fake-wheel", manifest=manifest, configuration=configuration)
