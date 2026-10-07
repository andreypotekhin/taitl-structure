from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkStatefulTransformRecipe:
    key: PySparkExpressionRecipe | tuple[PySparkExpressionRecipe, ...]
    processor: Any
    processor_mode: str
    input_schema: type[Schema]
    key_schema: type[Schema]
    state_schema: type[Schema] | None
    state_schemas: tuple[type[Schema], ...]
    output_schema: type[Schema]
    output_mode: str
    time_mode: str
    interface: str = "row"
    event_time_column: str | None = None
    initial_state: object | None = None
    state_attributes: tuple[Any, ...] = ()
    initial_schema: type[Schema] | None = None
