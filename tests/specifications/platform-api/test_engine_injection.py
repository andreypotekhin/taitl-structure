import pytest

from structure.core.platforms.api import Platform
from structure.core.platforms.model.EngineManifest import EngineManifest
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.version import VERSION


class Stock:
    pass


class Replacement:
    pass


def test_engine_replacement_requires_an_explicit_global_opt_in() -> None:
    manifest = EngineManifest(VERSION, Platform.engine_revision(), {Stock: Replacement})
    resolver = Platform.resolve_engine()

    with pytest.raises(ValueError, match="allow_injection"):
        resolver(Stock, platform="fake", distribution="fake-wheel", manifest=manifest, configuration=PlatformConfiguration.resolve({}))

    enabled = PlatformConfiguration.resolve({"platform": {"plugin_options": "allow_injection"}})
    assert resolver(Stock, platform="fake", distribution="fake-wheel", manifest=manifest, configuration=enabled) is Replacement


def test_incompatible_manifest_never_falls_back_to_the_stock_engine() -> None:
    manifest = EngineManifest(VERSION, "old", {Stock: Replacement})
    configuration = PlatformConfiguration.resolve({"platform": {"plugin_options": "allow_injection"}})

    with pytest.raises(ValueError, match="engine revision"):
        Platform.resolve_engine()(Stock, platform="fake", distribution="fake-wheel", manifest=manifest, configuration=configuration)
