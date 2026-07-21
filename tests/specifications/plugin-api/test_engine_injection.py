import pytest

from structure.core.plugins.api import Plugin
from structure.core.plugins.model.EngineManifest import EngineManifest
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.version import VERSION


class Stock:
    pass


class Replacement:
    pass


def test_engine_replacement_requires_an_explicit_global_opt_in() -> None:
    manifest = EngineManifest(VERSION, Plugin.engine_revision(), {Stock: Replacement})
    resolver = Plugin.resolve_engine()

    with pytest.raises(ValueError, match="allow_injection"):
        resolver(
            Stock,
            plugin="fake",
            distribution="fake-wheel",
            manifest=manifest,
            configuration=PluginConfiguration.resolve({}),
        )

    enabled = PluginConfiguration.resolve({"plugin": {"plugin_options": "allow_injection"}})
    assert (
        resolver(Stock, plugin="fake", distribution="fake-wheel", manifest=manifest, configuration=enabled)
        is Replacement
    )


def test_incompatible_manifest_never_falls_back_to_the_stock_engine() -> None:
    manifest = EngineManifest(VERSION, "old", {Stock: Replacement})
    configuration = PluginConfiguration.resolve({"plugin": {"plugin_options": "allow_injection"}})

    with pytest.raises(ValueError, match="engine revision"):
        Plugin.resolve_engine()(
            Stock, plugin="fake", distribution="fake-wheel", manifest=manifest, configuration=configuration
        )
