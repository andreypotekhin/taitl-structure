from functools import wraps
from importlib import import_module

from structure.plugin.pyspark.dsl.expressions import *  # noqa: F403

_pyspark_expressions = import_module("structure.plugin.pyspark.dsl.expressions")


def _compatibility(function):
    @wraps(function)
    def compatibility(*args, **kwargs):
        return function(*args, **kwargs)

    compatibility.__module__ = __name__
    return compatibility


for _name, _value in vars(_pyspark_expressions).items():
    if not _name.startswith("_") and callable(_value):
        globals()[_name] = _compatibility(_value)
