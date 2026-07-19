from dataclasses import dataclass

from structure.platform.api import PlatformDescriptor
from structure.platform.api.v1 import PlatformAPI


@dataclass(frozen=True)
class SelectedPlatform:
    descriptor: PlatformDescriptor
    api_version: int
    api: PlatformAPI
