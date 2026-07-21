from typing import Protocol


class SerializationAPI(Protocol):
    def encode(self, payload: object) -> bytes: ...

    def decode(self, payload: bytes) -> object: ...
