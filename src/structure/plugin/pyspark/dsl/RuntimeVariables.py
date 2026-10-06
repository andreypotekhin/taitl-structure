from contextvars import ContextVar
from typing import Mapping

_runtime_variables: ContextVar[Mapping[str, object]] = ContextVar("structure_pyspark_runtime_variables", default={})


def bind_runtime_variables(values: Mapping[str, object]):
    """Bind invocation variables while an online Spark plan is evaluated."""
    return _runtime_variables.set(values)


def reset_runtime_variables(token) -> None:
    _runtime_variables.reset(token)


def runtime_variable(name: str) -> object:
    values = _runtime_variables.get()
    try:
        return values[name]
    except KeyError as error:
        raise ValueError(f"Runtime variable {name!r} was not supplied to this transform invocation") from error


def invocation_variables(invocation) -> dict[str, object]:
    """Resolve and validate the variable bindings declared by an invocation."""
    result = {}
    for name, declaration in getattr(type(invocation), "_structure_variables", {}).items():
        bound = getattr(invocation, "_structure_bound_variables", {})
        if name not in bound and declaration.required:
            raise TypeError(f"Runtime variable {name!r} is required for {type(invocation).__name__}")
        value = bound.get(name, declaration.default)
        result[name] = declaration.validate(value)
    return result
