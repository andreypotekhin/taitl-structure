from collections.abc import Mapping
from typing import cast

from structure.plugin.api.v1 import CapabilitiesAPI as CapabilitiesAPIV1
from structure.plugin.api.v1.model import BackendCapabilities


class Capabilities(CapabilitiesAPIV1):
    """Minimal capability facet; a production plugin must return a complete capability model."""

    def resolve(self, *, options: Mapping[str, object]) -> BackendCapabilities:
        return cast(BackendCapabilities, object())
