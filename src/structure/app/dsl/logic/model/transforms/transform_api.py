from __future__ import annotations

from typing import Callable
import inspect

from structure.app.dsl.logic.model.expr.expressions import literal
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.CompileContext import current_context
from structure.app.dsl.logic.model.transforms.ExprFunction import ExprFunction
from structure.app.dsl.logic.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.logic.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.logic.model.transforms.SchemaMode import SchemaMode
from structure.app.dsl.logic.model.transforms.Transform import Transform


def input(schema: type[Structure]) -> InputDeclaration:
    if not isinstance(schema, type) or not issubclass(schema, Structure):
        raise TypeError("input(...) requires a Structure schema class")
    return InputDeclaration(schema=schema)


def output(schema: type[Structure]) -> OutputDeclaration:
    if not isinstance(schema, type) or not issubclass(schema, Structure):
        raise TypeError("output(...) requires a Structure schema class")
    return OutputDeclaration(schema=schema)


def transform(target=None, **kwargs):
    def decorate(item):
        if inspect.isclass(item):
            return _decorate_transform_class(item, kwargs)
        if inspect.isfunction(item):
            return _decorate_transform_method(item, kwargs)
        raise TypeError("@transform can decorate a Transform class or terminal output method")

    if target is None:
        return decorate
    if kwargs:
        return decorate(target)
    return decorate(target)


def expr_fn(function: Callable) -> ExprFunction:
    return ExprFunction(function)


def _decorate_transform_class(cls, kwargs):
    allowed = {"validate_intermediate", "streaming_compatible", "to"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown class option(s): {', '.join(sorted(unknown))}")
    if "to" in kwargs and not (isinstance(kwargs["to"], type) and issubclass(kwargs["to"], Structure)):
        raise TypeError("@transform(to=...) on a class requires a Structure schema class")
    if not issubclass(cls, Transform):
        raise TypeError("@transform classes must inherit from Transform")
    cls._structure_transform = True
    cls._structure_transform_options = dict(kwargs)
    return cls


def _decorate_transform_method(function, kwargs):
    allowed = {"output", "source"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown method option(s): {', '.join(sorted(unknown))}")
    if "output" not in kwargs:
        raise TypeError("@transform on a method requires output=output_declaration")
    if not isinstance(kwargs["output"], OutputDeclaration):
        raise TypeError("@transform(output=...) on a method requires an output(...) declaration")
    setattr(
        function,
        "_structure_output_method",
        {
            "output": kwargs["output"],
            "source": kwargs.get("source"),
        },
    )
    return function


def before(
    target: Callable,
    *,
    pass_inputs: bool = False,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    project_output: bool = False,
    streaming_safe: bool = False,
):
    return _hook(
        "before",
        target,
        pass_inputs=pass_inputs,
        schema_mode=schema_mode,
        project_output=project_output,
        streaming_safe=streaming_safe,
    )


def after(
    target: Callable,
    *,
    pass_inputs: bool = False,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    project_output: bool = False,
    streaming_safe: bool = False,
):
    return _hook(
        "after",
        target,
        pass_inputs=pass_inputs,
        schema_mode=schema_mode,
        project_output=project_output,
        streaming_safe=streaming_safe,
    )


def where(predicate: object) -> None:
    context = current_context()
    if context is None:
        raise RuntimeError("where(...) can only be used inside a compiled Structure subtransform")
    context.filters.append(literal(predicate))


def _hook(
    phase: str,
    target: Callable,
    *,
    pass_inputs: bool,
    schema_mode: SchemaMode,
    project_output: bool,
    streaming_safe: bool,
):
    if not callable(target):
        raise TypeError(f"@{phase}(...) requires a subtransform method")

    def decorate(function: Callable) -> Callable:
        setattr(
            function,
            "_structure_hook",
            {
                "phase": phase,
                "target": target.__name__,
                "pass_inputs": pass_inputs,
                "schema_mode": schema_mode,
                "project_output": project_output,
                "streaming_safe": streaming_safe,
            },
        )
        return function

    return decorate
