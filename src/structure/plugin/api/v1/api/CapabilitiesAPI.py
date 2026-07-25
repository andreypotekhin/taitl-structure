from collections.abc import Mapping
from typing import Protocol

from structure.plugin.api.v1.model import BackendCapabilities


class CapabilitiesAPI(Protocol):
    def resolve(self, *, options: Mapping[str, object]) -> BackendCapabilities: ...
