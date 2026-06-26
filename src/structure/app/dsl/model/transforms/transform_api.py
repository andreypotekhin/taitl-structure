from __future__ import annotations

import inspect
from typing import Callable, Iterable, cast, overload

from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.expr.InputScope import InputScope, join_one
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.BindingSelector import BindingSelector, SelectedDeclaration
from structure.app.dsl.model.transforms.ExprFunction import ExprFunction
from structure.app.dsl.model.transforms.InOutBinding import InOutBinding
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode
from structure.app.dsl.model.transforms.Transform import Transform


@overload
def input(value: type[Structure]) -> InputDeclaration: ...


@overload
def input(value: InputDeclaration) -> BindingSelector: ...


def input(value: type[Structure] | InputDeclaration) -> InputDeclaration | BindingSelector:
    if isinstance(value, InputDeclaration):
        return BindingSelector("input", value)
    if not isinstance(value, type) or not issubclass(value, Structure):
        raise TypeError("input(...) requires a Structure schema class")
    return InputDeclaration(schema=value)


@overload
def output(value: type[Structure]) -> OutputDeclaration: ...


@overload
def output(value: OutputDeclaration) -> BindingSelector: ...


def output(value: type[Structure] | OutputDeclaration) -> OutputDeclaration | BindingSelector:
    if isinstance(value, OutputDeclaration):
        return BindingSelector("output", value)
    if not isinstance(value, type) or not issubclass(value, Structure):
        raise TypeError("output(...) requires a Structure schema class")
    return OutputDeclaration(schema=value)


@overload
def lane(value: type[Structure]) -> LaneDeclaration: ...


@overload
def lane(value: SelectedDeclaration) -> BindingSelector: ...


def lane(value: type[Structure] | SelectedDeclaration) -> LaneDeclaration | BindingSelector:
    if isinstance(value, (InputDeclaration, LaneDeclaration, OutputDeclaration)):
        return BindingSelector("lane", value)
    if not isinstance(value, type) or not issubclass(value, Structure):
        raise TypeError("lane(...) requires a Structure schema class")
    return LaneDeclaration(schema=value)


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
    kwargs = _normalize_method_options(kwargs)
    allowed = {"input", "output", "inout"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown method option(s): {', '.join(sorted(unknown))}")
    if not kwargs:
        raise TypeError("@transform on a method requires input=..., output=..., or inout=...")
    if "inout" in kwargs and ("input" in kwargs or "output" in kwargs):
        raise TypeError("@transform on a method cannot combine inout=... with input=... or output=...")

    inputs = _method_declarations(
        kwargs,
        name="input",
        bare=(InputDeclaration, LaneDeclaration),
        roles={"input", "lane"},
    )
    outputs = _method_declarations(
        kwargs,
        name="output",
        bare=(LaneDeclaration, OutputDeclaration),
        roles={"lane", "output"},
    )
    if "inout" in kwargs:
        binding = kwargs["inout"]
        if not isinstance(binding, InOutBinding):
            raise TypeError("@transform(inout=...) requires a pipe binding such as source | target")
        inputs = _method_declaration_values(
            binding.inputs,
            option="@transform(inout=...) input side",
            bare=(InputDeclaration, LaneDeclaration),
            roles={"input", "lane"},
        )
        outputs = _method_declaration_values(
            binding.outputs,
            option="@transform(inout=...) output side",
            bare=(LaneDeclaration, OutputDeclaration),
            roles={"lane", "output"},
        )
    if len(set(map(_binding_key, inputs))) != len(inputs):
        raise TypeError("@transform(input=...) cannot repeat a declaration")
    if len(set(map(_binding_key, outputs))) != len(outputs):
        raise TypeError("@transform(output=...) cannot repeat a declaration")
    setattr(
        function,
        "_structure_output_method",
        {
            "inputs": inputs,
            "outputs": outputs,
        },
    )
    return function


def _normalize_method_options(kwargs: dict[str, object]) -> dict[str, object]:
    recycled = {"inputs", "outputs", "lane", "lanes", "in", "in_", "out"} & set(kwargs)
    if recycled:
        names = ", ".join(sorted(recycled))
        raise TypeError(f"@transform method option(s) {names} were recycled; use input=..., output=..., or inout=...")
    return dict(kwargs)


def _method_declarations(kwargs, *, name: str, bare: tuple[type, ...], roles: set[str]) -> tuple:
    if name not in kwargs or kwargs[name] is None:
        return ()
    return _method_declaration_values(kwargs[name], option=f"@transform({name}=...)", bare=bare, roles=roles)


def _method_declaration_values(value: object, *, option: str, bare: tuple[type, ...], roles: set[str]) -> tuple:
    if _valid_binding(value, bare=bare, roles=roles):
        return (value,)
    if isinstance(value, (InputDeclaration, LaneDeclaration, OutputDeclaration, BindingSelector)):
        raise TypeError(f"{option} requires {_declaration_kinds(bare, roles)} declarations")
    values = _declaration_sequence(value, option=option)
    if not all(_valid_binding(item, bare=bare, roles=roles) for item in values):
        raise TypeError(f"{option} requires {_declaration_kinds(bare, roles)} declarations")
    return values


def _declarations(kwargs, *, singular: str, plural: str, allowed: tuple[type, ...]) -> tuple:
    if singular in kwargs and kwargs[singular] is not None:
        values = (kwargs[singular],)
    elif plural in kwargs and kwargs[plural] is not None:
        values = _declaration_sequence(kwargs[plural], option=f"@transform({plural}=...)")
    else:
        return ()
    if not all(isinstance(value, allowed) for value in values):
        raise TypeError(f"@transform({plural}=...) requires {_declaration_kinds(allowed)} declarations")
    return values


def _declaration_sequence(value: object, *, option: str) -> tuple:
    if isinstance(value, (str, bytes)):
        raise TypeError(f"{option} requires a non-empty declaration sequence")
    try:
        values: tuple[object, ...] = tuple(cast(Iterable[object], value))
    except TypeError as error:
        raise TypeError(f"{option} requires a declaration or non-empty declaration sequence") from error
    if not values:
        raise TypeError(f"{option} requires at least one declaration")
    return values


def _valid_binding(value: object, *, bare: tuple[type, ...], roles: set[str]) -> bool:
    if isinstance(value, bare):
        return True
    if not isinstance(value, BindingSelector) or value.role not in roles:
        return False
    if value.role == "input":
        return isinstance(value.declaration, InputDeclaration)
    if value.role == "lane":
        return isinstance(value.declaration, (InputDeclaration, LaneDeclaration, OutputDeclaration))
    if value.role == "output":
        return isinstance(value.declaration, OutputDeclaration)
    return False


def _binding_key(value: object) -> tuple[str, int]:
    if isinstance(value, BindingSelector):
        return value.role, id(value.declaration)
    return "bare", id(value)


def _declaration_kinds(allowed: tuple[type, ...], roles: set[str] | None = None) -> str:
    if allowed == (InputDeclaration,):
        return "input(...)"
    if allowed == (InputDeclaration, LaneDeclaration):
        return "input(...) or lane(...)"
    if allowed == (LaneDeclaration, OutputDeclaration):
        return "lane(...) or output(...)"
    return "input(...), lane(...), or output(...)"


def before(
    target: Callable,
    *,
    input: InputDeclaration | None = None,
    inputs: object | None = None,
    lane: InputDeclaration | LaneDeclaration | OutputDeclaration | None = None,
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
    lane: InputDeclaration | LaneDeclaration | OutputDeclaration | None = None,
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
    lane: InputDeclaration | LaneDeclaration | OutputDeclaration | None,
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
    return _declarations(
        kwargs, singular="output", plural="outputs", allowed=(InputDeclaration, LaneDeclaration, OutputDeclaration)
    )
