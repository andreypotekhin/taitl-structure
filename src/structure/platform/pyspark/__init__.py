__all__ = ["PySparkPlatform"]


def __getattr__(name: str):
    if name != "PySparkPlatform":
        raise AttributeError(name)
    from structure.platform.pyspark.Plugin import PySparkPlatform

    return PySparkPlatform
