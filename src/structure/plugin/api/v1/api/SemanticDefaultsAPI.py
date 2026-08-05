from collections.abc import Mapping
from typing import Protocol


class SemanticDefaultsAPI(Protocol):
    """Plugin-supplied defaults for Core-owned semantic policies."""

    def resolve(self, *, options: Mapping[str, object]) -> Mapping[str, object]: ...
