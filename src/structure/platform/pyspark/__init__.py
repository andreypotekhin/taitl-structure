from importlib import import_module
from typing import TYPE_CHECKING
from structure.platform.pyspark.PySparkPlatform import PySparkPlatform

if TYPE_CHECKING:
    from structure.platform.pyspark.dsl.field import (
        array,
        boolean,
        date,
        decimal,
        double,
        float,
        integer,
        long,
        map,
        string,
        struct,
        timestamp,
    )

__all__ = [
    "PySpark",
    "PySparkPlatform",
    "array",
    "boolean",
    "date",
    "decimal",
    "double",
    "field",
    "float",
    "integer",
    "long",
    "map",
    "string",
    "struct",
    "timestamp",
    "types",
]


def __getattr__(name: str):
    if name == "PySpark":
        from structure.platform.pyspark.api.PySpark import PySpark

        return PySpark
    if name in {"field", "types"}:
        return import_module(f"structure.platform.pyspark.dsl.{name}")
    if name in {"array", "boolean", "date", "decimal", "double", "float", "integer", "long", "map", "string", "struct", "timestamp"}:
        return getattr(import_module("structure.platform.pyspark.dsl.field"), name)
    dsl = import_module("structure.platform.pyspark.dsl")

    try:
        return getattr(dsl, name)
    except AttributeError as error:
        raise AttributeError(name) from error
