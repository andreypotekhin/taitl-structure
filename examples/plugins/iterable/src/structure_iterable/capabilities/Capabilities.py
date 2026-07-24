from typing import cast

from structure.plugin.api.v1.model import BackendCapabilities


class Capabilities:
    """Minimal capability facet; a production plugin must return a complete capability model."""

    def resolve(self, *, profile: str, variant: str) -> BackendCapabilities:
        return cast(BackendCapabilities, object())
