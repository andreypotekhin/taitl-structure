from dataclasses import dataclass
from typing import Callable

from structure.platform.api import PlatformPlugin


@dataclass(frozen=True)
class DiscoveredPlatform:
    name: str
    distribution: str
    load: Callable[[], PlatformPlugin]
