from __future__ import annotations

import inspect
from typing import Callable

from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.expr.InputScope import InputScope, join_one
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.ExprFunction import ExprFunction
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode
from structure.app.dsl.model.transforms.Transform import Transform


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
        raise TypeError("@transform can decorate a Transform class or transform method")

    if target is None:
        return decorate
    if kwargs:
        return decorate(target)
    return decorate(target)


def expr_fn(function: Callable) -> ExprFunction:
    return ExprFunction(function)


def _decorate_transform_class(cls, kwargs):
    allowed = {"validate_intermediate", "streaming_compatible"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown class option(s): {', '.join(sorted(unknown))}")
    if not issubclass(cls, Transform):
        raise TypeError("@transform classes must inherit from Transform")
    cls._structure_transform = True
    cls._structure_transform_options = dict(kwargs)
    return cls


def _decorate_transform_method(function, kwargs):
    allowed = {"input", "inputs", "output", "outputs"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown method option(s): {', '.join(sorted(unknown))}")
    if not kwargs:
        raise TypeError("@transform on a method requires input(s)=... or output(s)=...")
    if "input" in kwargs and "inputs" in kwargs:
        raise TypeError("@transform on a method cannot use both input= and inputs=")
    if "output" in kwargs and "outputs" in kwargs:
        raise TypeError("@transform on a method cannot use both output= and outputs=")

    inputs = _declarations(kwargs, singular="input", plural="inputs", allowed=(InputDeclaration, OutputDeclaration))
    outputs = _declarations(kwargs, singular="output", plural="outputs", allowed=(OutputDeclaration,))
    if len(set(map(id, inputs))) != len(inputs):
        raise TypeError("@transform(inputs=...) cannot repeat a declaration")
    if len(set(map(id, outputs))) != len(outputs):
        raise TypeError("@transform(outputs=...) cannot repeat a declaration")
    if len(inputs) == 1 and isinstance(inputs[0], OutputDeclaration) and not outputs:
        raise TypeError("@transform(input=...) with an output lane requires output=output_declaration")
    setattr(
        function,
        "_structure_output_method",
        {
            "input": inputs[0] if len(inputs) == 1 else None,
            "output": outputs[0] if len(outputs) == 1 else None,
            "inputs": inputs,
            "outputs": outputs,
        },
    )
    return function


def _declarations(kwargs, *, singular: str, plural: str, allowed: tuple[type, ...]) -> tuple:
    if singular in kwargs:
        values = (kwargs[singular],)
    elif plural in kwargs:
        value = kwargs[plural]
        if isinstance(value, (str, bytes)):
            raise TypeError(f"@transform({plural}=...) requires a non-empty declaration sequence")
        try:
            values = tuple(value)
        except TypeError as error:
            raise TypeError(f"@transform({plural}=...) requires a non-empty declaration sequence") from error
        if not values:
            raise TypeError(f"@transform({plural}=...) requires at least one declaration")
    else:
        return ()
    if not all(isinstance(value, allowed) for value in values):
        kinds = "input(...) or output(...)" if InputDeclaration in allowed else "output(...)"
        raise TypeError(f"@transform({plural}=...) requires {kinds} declarations")
    return values


def before(
    target: Callable,
    *,
    lane: InputDeclaration | OutputDeclaration,
    pass_inputs: bool = False,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    project_output: bool = False,
    streaming_safe: bool = False,
):
    return _hook(
        "before",
        target,
        lane=lane,
        pass_inputs=pass_inputs,
        schema_mode=schema_mode,
        project_output=project_output,
        streaming_safe=streaming_safe,
    )


def after(
    target: Callable,
    *,
    lane: InputDeclaration | OutputDeclaration,
    pass_inputs: bool = False,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    project_output: bool = False,
    streaming_safe: bool = False,
):
    return _hook(
        "after",
        target,
        lane=lane,
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
    lane: InputDeclaration | OutputDeclaration,
    pass_inputs: bool,
    schema_mode: SchemaMode,
    project_output: bool,
    streaming_safe: bool,
):
    if not callable(target):
        raise TypeError(f"@{phase}(...) requires a subtransform method")
    if not isinstance(lane, (InputDeclaration, OutputDeclaration)):
        raise TypeError(f"@{phase}(..., lane=...) requires an input(...) or output(...) declaration")

    def decorate(function: Callable) -> Callable:
        setattr(
            function,
            "_structure_hook",
            {
                "phase": phase,
                "target": target.__name__,
                "lane": lane,
                "pass_inputs": pass_inputs,
                "schema_mode": schema_mode,
                "project_output": project_output,
                "streaming_safe": streaming_safe,
            },
        )
        return function

    return decorate
