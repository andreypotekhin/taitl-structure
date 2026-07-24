from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import cast

from structure.plugin.api import PluginDescriptor
from structure.plugin.api.v1 import (
    AuthoringAPI,
    CapabilitiesAPI,
    CompilerAPI,
    ExecutionAPI,
    ExecutionRequest,
    PluginAPI,
    PluginCompilation,
    SchemaAPI,
    SerializationAPI,
)


@dataclass(frozen=True)
class IterableRelation:
    rows: tuple[dict[str, object], ...]

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, object]]) -> "IterableRelation":
        return cls(tuple(dict(row) for row in rows))

    def collect(self) -> list[dict[str, object]]:
        return [dict(row) for row in self.rows]


class IterableCompiler:
    def compile(self, request: object) -> PluginCompilation:
        return PluginCompilation(lowered={"operation": "identity"}, fingerprint="iterable-identity-v1")


class IterableExecutor:
    def execute(self, request: ExecutionRequest) -> IterableRelation:
        if not isinstance(request.runtime, Iterable):
            raise TypeError("The iterable fixture runtime must be a finite iterable of row mappings.")
        return IterableRelation.from_rows(cast(Iterable[Mapping[str, object]], request.runtime))


class IterableSerializer:
    def encode(self, payload: object) -> bytes:
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

    def decode(self, payload: bytes) -> object:
        return json.loads(payload.decode())


class IterablePlugin:
    descriptor = PluginDescriptor("iterable", "Iterable Fixture", "structure-iterable-fixture", "0.1.0", 1, 1)

    @classmethod
    def api(cls, version: int) -> PluginAPI:
        if version != 1:
            raise ValueError(f"Iterable fixture does not support Plugin API v{version}.")
        return PluginAPI(
            schema=cast(SchemaAPI, object()),
            authoring=cast(AuthoringAPI, object()),
            compiler=cast(CompilerAPI, IterableCompiler()),
            capabilities=cast(CapabilitiesAPI, object()),
            executor=cast(ExecutionAPI, IterableExecutor()),
            serializer=cast(SerializationAPI, IterableSerializer()),
        )
