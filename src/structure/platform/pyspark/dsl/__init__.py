from importlib import import_module

__all__ = ["field", "types"]


def __getattr__(name: str):
    if name in {"field", "types"}:
        return import_module(f"structure.platform.pyspark.dsl.{name}")
    core_dsl = import_module("structure.core.dsl.api")
    try:
        return getattr(core_dsl, name)
    except AttributeError as error:
        raise AttributeError(name) from error
