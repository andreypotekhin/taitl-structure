from typing import Protocol

from structure.plugin.api.v1.model import GenerationRequest, GenerationResult


class GenerationAPI(Protocol):
    def generate(self, request: GenerationRequest) -> GenerationResult: ...
