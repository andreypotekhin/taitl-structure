from typing import Mapping, Protocol

from structure.plugin.api.v1.model import GenerationRequest


class GenerationAPI(Protocol):
    def generate(self, request: GenerationRequest) -> Mapping[str, str]: ...
