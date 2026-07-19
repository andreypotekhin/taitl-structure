from importlib import import_module

__all__ = ["PySparkPlatform"]


def __getattr__(name: str):
    if name == "PySparkPlatform":
        from structure.platform.pyspark.Plugin import PySparkPlatform

        return PySparkPlatform
    dsl = import_module("structure.platform.pyspark.dsl")

    try:
        return getattr(dsl, name)
    except AttributeError as error:
        raise AttributeError(name) from error
