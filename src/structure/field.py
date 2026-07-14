"""Schema-field factories for declaration-only source modules."""

import sys
from types import ModuleType
from typing import NoReturn

from structure.app.dsl.model.schemas.schema_api import (
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
    "array",
    "boolean",
    "date",
    "decimal",
    "double",
    "float",
    "integer",
    "long",
    "map",
    "string",
    "struct",
    "timestamp",
]


class _FieldModule(ModuleType):

    def __call__(self, *args: object, **kwargs: object) -> NoReturn:
        raise TypeError(
            "field(...) is no longer supported. Use field.string(...), field.decimal(...), field.array(...), "
            "field.map(...), or field.struct(...). See docs/reference/Schema.ref.md."
        )


sys.modules[__name__].__class__ = _FieldModule
