from dataclasses import dataclass
from typing import cast

import pytest

from structure.plugin.api import PluginDescriptor
from structure.plugin.api.conformance import PluginConformance
from structure.plugin.api.v1 import AuthoringAPI, CapabilitiesAPI, CompilerAPI, PluginAPI, SchemaAPI


class Facet:
    pass


@dataclass
class FakePlugin:
    descriptor: PluginDescriptor
    api_value: object

    def api(self, version: int) -> object:
        return self.api_value


def complete_api() -> PluginAPI:
    return PluginAPI(
        schema=cast(SchemaAPI, Facet()),
        authoring=cast(AuthoringAPI, Facet()),
        compiler=cast(CompilerAPI, Facet()),
        capabilities=cast(CapabilitiesAPI, Facet()),
    )


def test_conformance_negotiates_the_highest_mutual_version_without_core_imports() -> None:
    plugin = FakePlugin(PluginDescriptor("fixture", "Fixture", "fixture-wheel", "1.0", 1, 2), complete_api())

    result = PluginConformance.negotiate(
        plugin,
        entry_name="fixture",
        distribution="fixture-wheel",
        minimum_api_version=1,
        maximum_api_version=2,
    )

    assert result.api_version == 2
    assert result.descriptor is plugin.descriptor


@pytest.mark.parametrize(("api", "message"), [(object(), "missing PluginAPI façade"), (None, "missing authoring")])
def test_conformance_reports_the_missing_contract_piece(api: object | None, message: str) -> None:
    if api is None:
        api = complete_api()
        object.__setattr__(api, "authoring", None)
    plugin = FakePlugin(PluginDescriptor("fixture", "Fixture", "fixture-wheel", "1.0", 1, 1), api)

    with pytest.raises(ValueError, match=message):
        PluginConformance.negotiate(plugin, entry_name="fixture", distribution="fixture-wheel")
