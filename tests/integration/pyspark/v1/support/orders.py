"""Compatibility import surface for the consolidated orders integration model."""

from integration.pyspark.v2.support.orders import generated_schemas as generated_order_schemas
from integration.pyspark.v2.support.orders import (
    input_frames,
    run_generated_transform,
    run_online_transform,
    source_schema_modules,
    transform,
)

__all__ = [
    "generated_order_schemas",
    "input_frames",
    "run_generated_transform",
    "run_online_transform",
    "source_schema_modules",
    "transform",
]
