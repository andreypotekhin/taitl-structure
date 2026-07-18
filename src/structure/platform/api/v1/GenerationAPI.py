from typing import Mapping, Protocol


class GenerationAPI(Protocol):
    def generate(self, payload: object) -> Mapping[str, str]: ...
