import inspect
from functools import wraps
from importlib import import_module

from structure.platform.pyspark.dsl.operations_api import *  # noqa: F403

_pyspark_operations = import_module("structure.platform.pyspark.dsl.operations_api")


def _compatibility(function):
    @wraps(function)
    def compatibility(*args, **kwargs):
        return function(*args, **kwargs)

    compatibility.__module__ = __name__
    return compatibility


for _name, _value in vars(_pyspark_operations).items():
    if not _name.startswith("_") and inspect.isfunction(_value):
        globals()[_name] = _compatibility(_value)
