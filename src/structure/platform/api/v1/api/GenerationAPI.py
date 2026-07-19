from typing import Mapping, Protocol

from structure.platform.api.v1.model import GenerationRequest


class GenerationAPI(Protocol):
    def generate(self, request: GenerationRequest) -> Mapping[str, str]: ...
