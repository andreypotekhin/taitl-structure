from dataclasses import dataclass
from typing import Mapping, Protocol


@dataclass(frozen=True)
class CompileRequest:
    transform: object
    target: str
    configuration: Mapping[str, object]


@dataclass(frozen=True)
class PlatformCompilation:
    lowered: object
    fingerprint: str
    analysis: object | None = None


class SchemaAPI(Protocol):
    def materialize(self, schema: object) -> object: ...


class CompilerAPI(Protocol):
    def compile(self, request: CompileRequest) -> PlatformCompilation: ...


class CapabilitiesAPI(Protocol):
    def supports(self, capability: str) -> bool: ...


class ExecutionAPI(Protocol):
    def execute(self, payload: object, runtime: object) -> object: ...


class GenerationAPI(Protocol):
    def generate(self, payload: object) -> Mapping[str, str]: ...


class SerializationAPI(Protocol):
    def encode(self, payload: object) -> bytes: ...

    def decode(self, payload: bytes) -> object: ...


@dataclass(frozen=True)
class PlatformAPI:
    schema: SchemaAPI
    compiler: CompilerAPI
    capabilities: CapabilitiesAPI
    executor: ExecutionAPI | None = None
    generator: GenerationAPI | None = None
    serializer: SerializationAPI | None = None
