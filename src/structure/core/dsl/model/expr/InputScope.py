from functools import wraps
from importlib import import_module

from structure.platform.pyspark.dsl.InputScope import (
    InputScope,
    as_of_one,
    cross_join,
    exists,
    full_join,
    inner_join,
    left_join,
    lookup_join,
    not_exists,
    right_join,
    rowset_join,
    temporal_one,
)

_pyspark_input_scope = import_module("structure.platform.pyspark.dsl.InputScope")

__all__ = [
    "InputScope",
    "as_of_one",
    "cross_join",
    "exists",
    "full_join",
    "inner_join",
    "left_join",
    "lookup_join",
    "not_exists",
    "right_join",
    "rowset_join",
    "temporal_one",
]


def _compatibility(function):
    @wraps(function)
    def compatibility(*args, **kwargs):
        return function(*args, **kwargs)

    compatibility.__module__ = __name__
    return compatibility


for _name in __all__[1:]:
    globals()[_name] = _compatibility(getattr(_pyspark_input_scope, _name))
