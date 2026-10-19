"""Typed handle for metrics captured by a Spark observation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Generic, TypeVar, cast

from structure.core.dsl.model.schemas.Schema import Schema

_SchemaValue = TypeVar("_SchemaValue", bound=Schema)


class ObservationResult(Generic[_SchemaValue]):
    """Read a fresh schema value from a native observation handle."""

    def __init__(self, schema: type[_SchemaValue], read: Callable[[], Mapping[str, Any]]) -> None:
        self._structure_schema = schema
        self._structure_read = read

    @property
    def schema(self) -> type[_SchemaValue]:
        """The declared metric Schema class."""
        return self._structure_schema

    def get(self) -> _SchemaValue:
        """Return a detached snapshot of the current native metrics."""
        values = dict(self._structure_read())
        missing = set(self._structure_schema._structure_fields) - set(values)
        if missing:
            raise RuntimeError(
                f"Observation metrics are not available yet; run an action that traverses this observation first "
                f"(missing: {', '.join(sorted(missing))})."
            )
        return cast(_SchemaValue, cast(Any, self._structure_schema)(
            **{
                name: _snapshot(values[name], field.type)
                for name, field in self._structure_schema._structure_fields.items()
            }
        ))


def _snapshot(value: Any, logical_type: object) -> Any:
    if value is None:
        return None
    if hasattr(logical_type, "schema"):
        schema = logical_type.schema
        mapping = _mapping(value)
        return schema(
            **{
                name: _snapshot(mapping[name], field.type)
                for name, field in schema._structure_fields.items()
            }
        )
    if hasattr(logical_type, "element"):
        return [_snapshot(item, logical_type.element) for item in value]
    if hasattr(logical_type, "key") and hasattr(logical_type, "value"):
        return {
            _snapshot(key, logical_type.key): _snapshot(item, logical_type.value)
            for key, item in value.items()
        }
    if isinstance(value, Mapping):
        return {key: _snapshot(item, logical_type) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_snapshot(item, logical_type) for item in value]
    if hasattr(value, "asDict"):
        return {key: _snapshot(item, logical_type) for key, item in value.asDict(recursive=True).items()}
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True)
    raise TypeError(f"Expected a native struct value, got {type(value).__name__}.")
