from dataclasses import dataclass

import pytest

from structure.core.platform import PlatformRegistry
from structure.platform.api import PlatformDescriptor
from structure.platform.api.v1 import PlatformAPI


@dataclass
class Distribution:
    name: str


class Entry:
    group = "structure.platform"

    def __init__(self, name, distribution, loader):
        self.name = name
        self.dist = Distribution(distribution)
        self._loader = loader

    def load(self):
        return self._loader()


class Facet:
    def materialize(self, schema):
        return schema

    def compile(self, request):
        return request

    def supports(self, capability):
        return True


class Plugin:
    def __init__(self, minimum=1, maximum=1):
        self.descriptor = PlatformDescriptor("fake", "Fake", "fake-wheel", "1.0", minimum, maximum)

    def api(self, version):
        return PlatformAPI(schema=Facet(), compiler=Facet(), capabilities=Facet())


def test_discovery_uses_metadata_without_loading_plugins() -> None:
    loaded = False

    def load():
        nonlocal loaded
        loaded = True
        return Plugin()

    registry = PlatformRegistry(lambda: [Entry("fake", "fake-wheel", load)])

    discovered = registry.discover()

    assert discovered[0].name == "fake"
    assert not loaded


@pytest.mark.parametrize("minimum,maximum", [(1, 1), (1, 2)])
def test_selection_negotiates_the_highest_mutual_v1_version(minimum, maximum) -> None:
    registry = PlatformRegistry(lambda: [Entry("fake", "fake-wheel", lambda: Plugin(minimum, maximum))])

    selected = registry.select("fake")

    assert selected.api_version == 1


def test_selection_rejects_an_incompatible_or_duplicate_platform() -> None:
    incompatible = PlatformRegistry(lambda: [Entry("fake", "fake-wheel", lambda: Plugin(2, 2))])
    duplicate = PlatformRegistry(lambda: [Entry("fake", "one", Plugin), Entry("fake", "two", Plugin)])

    with pytest.raises(ValueError, match="no compatible"):
        incompatible.select("fake")
    with pytest.raises(ValueError, match="multiple distributions"):
        duplicate.discover()
