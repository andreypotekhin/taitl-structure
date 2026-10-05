from importlib import import_module

from structure.plugin.pyspark.dsl.Interval import Interval
from structure.plugin.pyspark.dsl.Temporal import Temporal

_MODULES = (
    "joins",
    "operations",
    "generators",
    "relation_sets",
    "aggregation",
    "expressions",
    "operations_api",
    "InputScope",
    "body",
    "Stateful",
    "SinkRole",
    "TimeWindow",
    "Temporal",
    "Interval",
    "types",
    "field",
    "sql_api",
    "SqlResult",
)

__all__ = ["field", "types"]


def __getattr__(name: str):
    for module in _MODULES:
        value = getattr(import_module(f"structure.plugin.pyspark.dsl.{module}"), name, None)
        if value is not None:
            globals()[name] = value
            return value
    raise AttributeError(name)
