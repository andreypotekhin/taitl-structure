from __future__ import annotations

import inspect
from typing import Callable

from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.expr.InputScope import InputScope, join_one
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.ExprFunction import ExprFunction
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
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


def lane(schema: type[Structure]) -> LaneDeclaration:
    if not isinstance(schema, type) or not issubclass(schema, Structure):
        raise TypeError("lane(...) requires a Structure schema class")
    return LaneDeclaration(schema=schema)


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
    allowed = {"input", "inputs", "lane", "lanes", "output", "outputs"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown method option(s): {', '.join(sorted(unknown))}")
    if not kwargs:
        raise TypeError("@transform on a method requires input(s)=..., lane(s)=..., or output(s)=...")
    if "input" in kwargs and "inputs" in kwargs:
        raise TypeError("@transform on a method cannot use both input= and inputs=")
    if "lane" in kwargs and "lanes" in kwargs:
        raise TypeError("@transform on a method cannot use both lane= and lanes=")
    if ("input" in kwargs or "inputs" in kwargs) and ("lane" in kwargs or "lanes" in kwargs):
        raise TypeError("@transform on a method cannot mix input(s)=... with lane(s)=...")
    if "output" in kwargs and "outputs" in kwargs:
        raise TypeError("@transform on a method cannot use both output= and outputs=")

    inputs = _declarations(kwargs, singular="input", plural="inputs", allowed=(InputDeclaration,))
    lanes = _declarations(kwargs, singular="lane", plural="lanes", allowed=(LaneDeclaration, OutputDeclaration))
    outputs = _declarations(kwargs, singular="output", plural="outputs", allowed=(LaneDeclaration, OutputDeclaration))
    if len(set(map(id, inputs))) != len(inputs):
        raise TypeError("@transform(inputs=...) cannot repeat a declaration")
    if len(set(map(id, lanes))) != len(lanes):
        raise TypeError("@transform(lanes=...) cannot repeat a declaration")
    if len(set(map(id, outputs))) != len(outputs):
        raise TypeError("@transform(outputs=...) cannot repeat a declaration")
    setattr(
        function,
        "_structure_output_method",
        {
            "input": inputs[0] if len(inputs) == 1 else None,
            "lane": lanes[0] if len(lanes) == 1 else None,
            "output": outputs[0] if len(outputs) == 1 else None,
            "inputs": inputs,
            "lanes": lanes,
            "outputs": outputs,
        },
    )
    return function


def _declarations(kwargs, *, singular: str, plural: str, allowed: tuple[type, ...]) -> tuple:
    if singular in kwargs and kwargs[singular] is not None:
        values = (kwargs[singular],)
    elif plural in kwargs and kwargs[plural] is not None:
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
        kinds = "input(...), lane(...), or output(...)"
        if allowed == (InputDeclaration,):
            kinds = "input(...) declarations"
        elif allowed in ((LaneDeclaration, OutputDeclaration), (InputDeclaration, LaneDeclaration, OutputDeclaration)):
            kinds = "lane(...) or output(...) declarations"
        raise TypeError(f"@transform({plural}=...) requires {kinds} declarations")
    return values


def before(
    target: Callable,
    *,
    input: InputDeclaration | None = None,
    inputs: object | None = None,
    lane: LaneDeclaration | OutputDeclaration | None = None,
    lanes: object | None = None,
    output: InputDeclaration | LaneDeclaration | OutputDeclaration | None = None,
    outputs: object | None = None,
    pass_inputs: bool = False,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    project_output: bool = False,
    streaming_safe: bool = False,
):
    return _hook(
        "before",
        target,
        input=input,
        inputs=inputs,
        lane=lane,
        lanes=lanes,
        output=output,
        outputs=outputs,
        pass_inputs=pass_inputs,
        schema_mode=schema_mode,
        project_output=project_output,
        streaming_safe=streaming_safe,
    )


def after(
    target: Callable,
    *,
    input: InputDeclaration | None = None,
    inputs: object | None = None,
    lane: LaneDeclaration | OutputDeclaration | None = None,
    lanes: object | None = None,
    output: LaneDeclaration | OutputDeclaration | None = None,
    outputs: object | None = None,
    pass_inputs: bool = False,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    project_output: bool = False,
    streaming_safe: bool = False,
):
    return _hook(
        "after",
        target,
        input=input,
        inputs=inputs,
        lane=lane,
        lanes=lanes,
        output=output,
        outputs=outputs,
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
    input: InputDeclaration | None,
    inputs: object | None,
    lane: LaneDeclaration | OutputDeclaration | None,
    lanes: object | None,
    output: InputDeclaration | LaneDeclaration | OutputDeclaration | None,
    outputs: object | None,
    pass_inputs: bool,
    schema_mode: SchemaMode,
    project_output: bool,
    streaming_safe: bool,
):
    if not callable(target):
        raise TypeError(f"@{phase}(...) requires a subtransform method")
    kwargs = {"input": input, "inputs": inputs, "lane": lane, "lanes": lanes, "output": output, "outputs": outputs}
    sources = _hook_sources(phase, kwargs)
    targets = _hook_outputs(phase, kwargs, default=sources)

    def decorate(function: Callable) -> Callable:
        setattr(
            function,
            "_structure_hook",
            {
                "phase": phase,
                "target": target.__name__,
                "lane": sources[0] if len(sources) == 1 else None,
                "lanes": sources,
                "output": targets[0] if len(targets) == 1 else None,
                "outputs": targets,
                "pass_inputs": pass_inputs,
                "schema_mode": schema_mode,
                "project_output": project_output,
                "streaming_safe": streaming_safe,
            },
        )
        return function

    return decorate


def _hook_sources(phase: str, kwargs: dict[str, object]) -> tuple:
    if kwargs["input"] is not None and kwargs["inputs"] is not None:
        raise TypeError(f"@{phase}(...) cannot use both input= and inputs=")
    if kwargs["lane"] is not None and kwargs["lanes"] is not None:
        raise TypeError(f"@{phase}(...) cannot use both lane= and lanes=")
    has_input = kwargs["input"] is not None or kwargs["inputs"] is not None
    has_lane = kwargs["lane"] is not None or kwargs["lanes"] is not None
    if has_input and has_lane:
        raise TypeError(f"@{phase}(...) cannot mix input(s)=... with lane(s)=...")
    if has_input:
        return _declarations(kwargs, singular="input", plural="inputs", allowed=(InputDeclaration,))
    if has_lane:
        return _declarations(
            kwargs, singular="lane", plural="lanes", allowed=(InputDeclaration, LaneDeclaration, OutputDeclaration)
        )
    raise TypeError(f"@{phase}(...) requires input(s)=... or lane(s)=...")


def _hook_outputs(phase: str, kwargs: dict[str, object], *, default: tuple) -> tuple:
    if kwargs["output"] is not None and kwargs["outputs"] is not None:
        raise TypeError(f"@{phase}(...) cannot use both output= and outputs=")
    if kwargs["output"] is None and kwargs["outputs"] is None:
        return default
    return _declarations(kwargs, singular="output", plural="outputs", allowed=(InputDeclaration, LaneDeclaration, OutputDeclaration))
