from __future__ import annotations

from dataclasses import dataclass

from structure import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkStatefulTransformRecipe:
    key: PySparkExpressionRecipe
    processor: object
    processor_mode: str
    input_schema: type[Schema]
    key_schema: type[Schema]
    state_schema: type[Schema] | None
    output_schema: type[Schema]
    output_mode: str
    time_mode: str
    event_time_column: str | None = None
    initial_state: object | None = None
