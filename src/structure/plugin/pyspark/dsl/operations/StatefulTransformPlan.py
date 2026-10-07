from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class StatefulTransformPlan:
    key: Expression | tuple[Expression, ...]
    processor: Any
    processor_mode: str
    input_schema: type[Schema]
    key_schema: type[Schema]
    state_schema: type[Schema] | None
    output_schema: type[Schema]
    output_mode: str
    time_mode: str
    interface: str = "row"
    event_time_column: str | None = None
    initial_state: object | None = None
    state_attributes: tuple[Any, ...] = ()
    initial_schema: type[Schema] | None = None
