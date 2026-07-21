from dataclasses import dataclass
from typing import Callable

from structure.plugin.api import Plugin


@dataclass(frozen=True)
class DiscoveredPlugin:
    name: str
    distribution: str
    load: Callable[[], Plugin]
