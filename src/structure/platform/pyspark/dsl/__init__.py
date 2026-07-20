from importlib import import_module

_MODULES = (
    "joins",
    "operations",
    "aggregation",
    "expressions",
    "operations_api",
    "InputScope",
    "body",
    "Projection",
    "TimeWindow",
    "types",
    "field",
)

__all__ = ["field", "types"]


def __getattr__(name: str):
    for module in _MODULES:
        value = getattr(import_module(f"structure.platform.pyspark.dsl.{module}"), name, None)
        if value is not None:
            globals()[name] = value
            return value
    raise AttributeError(name)
