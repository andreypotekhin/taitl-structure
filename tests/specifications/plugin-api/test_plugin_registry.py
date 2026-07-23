from dataclasses import dataclass
from typing import cast

import pytest

from structure.core.plugins.api import Plugin
from structure.core.plugins.logic.PluginRegistry import PluginRegistry
from structure.core.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.plugin.api import PluginDescriptor
from structure.plugin.api.v1 import PluginAPI


@dataclass
class Distribution:
    name: str


class Entry:
    group = "structure.plugin"

    def __init__(self, name, distribution, loader):
        self.name = name
        self.dist = Distribution(distribution)
        self._loader = loader

    def load(self):
        return self._loader()


class Facet:
    def validate(self, request):
        return None

    def materialize(self, schema):
        return schema

    def build(self, request):
        return request.payload

    def read(self, request):
        return request.schema

    def source(self, schema, *, to):
        return schema

    def compile(self, request):
        return request

    def resolve(self, *, profile, variant):
        return cast(BackendCapabilities, object())

    def open_step(self, request):
        raise AssertionError("This registry fixture does not author transform steps.")

    def result_arguments(self, results):
        return ()

    def rewrite_body(self, body, *, frames):
        return body


class FakePlugin:
    def __init__(self, minimum=1, maximum=1, *, name="fake", distribution="fake-wheel"):
        self.descriptor = PluginDescriptor(name, "Fake", distribution, "1.0", minimum, maximum)

    def api(self, version):
        return PluginAPI(schema=Facet(), compiler=Facet(), capabilities=Facet(), authoring=Facet())


def test_discovery_uses_metadata_without_loading_plugins() -> None:
    loaded = False

    def load():
        nonlocal loaded
        loaded = True
        return FakePlugin()

    registry = Plugin.registry(lambda: [Entry("fake", "fake-wheel", load)])

    discovered = registry.discover()

    assert discovered[0].name == "fake"
    assert not loaded


def test_plugin_api_creates_a_registry() -> None:
    registry = Plugin.registry(lambda: [Entry("fake", "fake-wheel", FakePlugin)])

    assert registry.select("fake").descriptor.name == "fake"


def test_source_checkout_discovers_bundled_pyspark_without_installed_entry_point(monkeypatch) -> None:
    monkeypatch.setattr("structure.core.plugins.logic.PluginRegistry.entry_points", lambda **_: ())

    selected = PluginRegistry().select("pyspark")

    assert selected.descriptor.distribution == "structure"


@pytest.mark.parametrize("minimum,maximum", [(1, 1), (1, 2)])
def test_selection_negotiates_the_highest_mutual_v1_version(minimum, maximum) -> None:
    registry = Plugin.registry(lambda: [Entry("fake", "fake-wheel", lambda: FakePlugin(minimum, maximum))])

    selected = registry.select("fake")

    assert selected.api_version == 1


def test_selection_downgrades_a_newer_core_to_the_highest_supported_plugin_version() -> None:
    registry = Plugin.registry(
        lambda: [Entry("fake", "fake-wheel", lambda: FakePlugin(1, 1))], minimum_api_version=1, maximum_api_version=2
    )

    assert registry.select("fake").api_version == 1


def test_selection_rejects_an_incompatible_or_duplicate_platform() -> None:
    incompatible = Plugin.registry(lambda: [Entry("fake", "fake-wheel", lambda: FakePlugin(2, 2))])
    duplicate = Plugin.registry(lambda: [Entry("fake", "one", FakePlugin), Entry("fake", "two", FakePlugin)])

    with pytest.raises(ValueError, match="no compatible"):
        incompatible.select("fake")
    with pytest.raises(ValueError, match="multiple distributions"):
        duplicate.discover()


def test_discovery_normalizes_disabled_distribution_names_without_loading_plugins() -> None:
    loaded = False

    def load():
        nonlocal loaded
        loaded = True
        return FakePlugin()

    registry = Plugin.registry(lambda: [Entry("fake", "Fake_Wheel", load)])

    assert registry.discover(disabled_distributions=frozenset({"fake-wheel"})) == ()
    assert not loaded


@pytest.mark.parametrize("name", ["Fake", "fake_name", "2fake"])
def test_discovery_rejects_invalid_platform_entry_point_names(name) -> None:
    registry = Plugin.registry(lambda: [Entry(name, "fake-wheel", FakePlugin)])

    with pytest.raises(ValueError, match="PLUGIN-E2706"):
        registry.discover()


def test_selection_reports_plugin_load_failure_without_a_traceback() -> None:
    def load():
        raise RuntimeError("missing optional dependency")

    registry = Plugin.registry(lambda: [Entry("fake", "fake-wheel", load)])

    with pytest.raises(ValueError, match="PLUGIN-E2705: Could not load.*RuntimeError: missing optional dependency"):
        registry.select("fake")


@pytest.mark.parametrize(
    ("plugin", "message"),
    [
        (FakePlugin(name="other"), "plugin named 'other'"),
        (FakePlugin(distribution="other-wheel"), "declares distribution 'other-wheel'"),
    ],
)
def test_selection_rejects_descriptor_identity_mismatches(plugin, message) -> None:
    registry = Plugin.registry(lambda: [Entry("fake", "fake-wheel", lambda: plugin)])

    with pytest.raises(ValueError, match=message):
        registry.select("fake")
