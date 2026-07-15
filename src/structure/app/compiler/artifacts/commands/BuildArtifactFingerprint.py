from __future__ import annotations

import hashlib
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any


class BuildArtifactFingerprint:

    def __call__(self, artifact) -> str:
        return hashlib.sha256(repr(self._value(artifact)).encode()).hexdigest()

    def _value(self, value: Any):
        if is_dataclass(value):
            fields_to_encode = (
                field
                for field in fields(value)
                if not (
                    type(value).__name__ == "CompiledTransform" and field.name in {"schemas", "semantic_fingerprint"}
                )
            )
            return (
                f"{type(value).__module__}.{type(value).__qualname__}",
                tuple((field.name, self._value(getattr(value, field.name))) for field in fields_to_encode),
            )
        if isinstance(value, Enum):
            return (f"{type(value).__module__}.{type(value).__qualname__}", value.value)
        if isinstance(value, type):
            return (value.__module__, value.__qualname__)
        if isinstance(value, dict):
            return tuple(sorted((self._value(key), self._value(item)) for key, item in value.items()))
        if isinstance(value, (list, tuple)):
            return tuple(self._value(item) for item in value)
        if isinstance(value, (set, frozenset)):
            return tuple(sorted((self._value(item) for item in value), key=repr))
        return value
