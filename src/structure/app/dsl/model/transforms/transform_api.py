from __future__ import annotations

import inspect
from typing import Any, Callable, Iterable, TypeVar, cast, overload

from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.ir.model.WatermarkPlan import WatermarkPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.expr.InputScope import InputScope, lookup_join
from structure.app.dsl.model.schemas.Projection import Projection
from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.transforms.BindingSelector import BindingSelector, SelectedDeclaration
from structure.app.dsl.model.transforms.InOutBinding import InOutBinding
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.operations import cache_operation
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode
from structure.app.dsl.model.transforms.SpecialFunction import SpecialFunction
from structure.app.dsl.model.transforms.StreamingMode import StreamingMode
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.types.BooleanType import BooleanType

Projected = TypeVar("Projected", bound=Schema)

_CLASS_OPTIONS = {"validate_intermediate", "streaming_compatible"}
_STEP_METHOD_OPTIONS = {"target_backend", "target_platform", "target_profile"}
_METHOD_BINDING_OPTIONS = {"input", "output", "inout"}
_METHOD_OPTIMIZATION_OPTIONS = {"cache"}


@overload
def input(value: type[Schema], *, streaming: StreamingMode = StreamingMode.NO) -> InputDeclaration: ...


@overload
def input(value: InputDeclaration) -> BindingSelector: ...


def input(
    value: type[Schema] | InputDeclaration,
    *,
    streaming: StreamingMode = StreamingMode.NO,
) -> InputDeclaration | BindingSelector:
    if isinstance(value, InputDeclaration):
        if streaming is not StreamingMode.NO:
            raise TypeError("input(existing_input, streaming=...) is invalid; set streaming on the declaration")
        return BindingSelector("input", value)
    if not isinstance(value, type) or not issubclass(value, Schema):
        raise TypeError("input(...) requires a Schema class")
    if not isinstance(streaming, StreamingMode):
        raise TypeError("input(streaming=...) requires a StreamingMode value")
    return InputDeclaration(schema=value, streaming=streaming)


@overload
def output(value: type[Schema]) -> OutputDeclaration: ...


@overload
def output(value: OutputDeclaration) -> BindingSelector: ...


def output(value: type[Schema] | OutputDeclaration) -> OutputDeclaration | BindingSelector:
    if isinstance(value, OutputDeclaration):
        return BindingSelector("output", value)
    if not isinstance(value, type) or not issubclass(value, Schema):
        raise TypeError("output(...) requires a Schema class")
    return OutputDeclaration(schema=value)


@overload
def lane(value: type[Schema]) -> LaneDeclaration: ...


@overload
def lane(value: SelectedDeclaration) -> BindingSelector: ...


def lane(value: type[Schema] | SelectedDeclaration) -> LaneDeclaration | BindingSelector:
    if isinstance(value, (InputDeclaration, LaneDeclaration, OutputDeclaration)):
        return BindingSelector("lane", value)
    if not isinstance(value, type) or not issubclass(value, Schema):
        raise TypeError("lane(...) requires a Schema class")
    return LaneDeclaration(schema=value)


def transform(target=None, **kwargs):
    def decorate(item):
        if inspect.isclass(item):
            return _decorate_transform_class(item, kwargs)
        if inspect.isfunction(item):
            raise TypeError(
                "@transform decorates Transform classes only; replace method-level @transform(...) with @step(...)."
            )
        raise TypeError("@transform can decorate a Transform class only")

    if target is None:
        return decorate
    if kwargs:
        return decorate(target)
    return decorate(target)


def step(target=None, **kwargs):
    def decorate(item):
        if not inspect.isfunction(item):
            raise TypeError("@step can decorate Transform methods only")
        return _decorate_transform_method(item, kwargs)

    if target is None:
        return decorate
    if kwargs:
        return decorate(target)
    return decorate(target)


def special(function: Callable | None = None, *, type: str, **kwargs):
    allowed = {"expr", "udf", "opaque"}
    if type not in allowed:
        raise TypeError(f"@special(type=...) must use one of: {', '.join(sorted(allowed))}")
    if type == "expr" and kwargs:
        unknown = ", ".join(sorted(kwargs))
        raise TypeError(f"@special(type=\"expr\") got unknown option(s): {unknown}")
    if type == "opaque" and kwargs:
        unknown = ", ".join(sorted(kwargs))
        raise TypeError(f"@special(type=\"opaque\") got unknown option(s): {unknown}")
    if type == "udf":
        unknown_options = set(kwargs) - {"return_type", "nullable"}
        if unknown_options:
            raise TypeError(f"@special(type=\"udf\") got unknown option(s): {', '.join(sorted(unknown_options))}")

    def decorate(target: Callable) -> SpecialFunction:
        return SpecialFunction(
            target,
            type=type,
            return_type=kwargs.get("return_type"),
            nullable=bool(kwargs.get("nullable", True)),
        )

    if function is None:
        return decorate
    return decorate(function)


def _decorate_transform_class(cls, kwargs):
    allowed = _CLASS_OPTIONS | _STEP_METHOD_OPTIONS
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown class option(s): {', '.join(sorted(unknown))}")
    if not issubclass(cls, Transform):
        raise TypeError("@transform classes must inherit from Transform")
    options = _normalize_transform_options(kwargs)
    cls._structure_transform = True
    cls._structure_transform_options = options
    cls._structure_step_method_options = _step_method_options(options)
    return cls


def _decorate_transform_method(function, kwargs):
    kwargs = _normalize_method_options(kwargs)
    allowed = _METHOD_BINDING_OPTIONS | _STEP_METHOD_OPTIONS | _METHOD_OPTIMIZATION_OPTIONS
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@step got unknown method option(s): {', '.join(sorted(unknown))}")
    if not kwargs:
        raise TypeError("@step on a method requires input=..., output=..., or inout=...")
    if "inout" in kwargs and ("input" in kwargs or "output" in kwargs):
        raise TypeError("@step on a method cannot combine inout=... with input=... or output=...")

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
            raise TypeError("@step(inout=...) requires a pipe binding such as source | target")
        inputs = _method_declaration_values(
            binding.inputs,
            option="@step(inout=...) input side",
            bare=(InputDeclaration, LaneDeclaration),
            roles={"input", "lane"},
        )
        outputs = _method_declaration_values(
            binding.outputs,
            option="@step(inout=...) output side",
            bare=(LaneDeclaration, OutputDeclaration),
            roles={"lane", "output"},
        )
    if len(set(map(_binding_key, inputs))) != len(inputs):
        raise TypeError("@step(input=...) cannot repeat a declaration")
    if len(set(map(_binding_key, outputs))) != len(outputs):
        raise TypeError("@step(output=...) cannot repeat a declaration")
    setattr(
        function,
        "_structure_output_method",
        {
            "inputs": inputs,
            "outputs": outputs,
            "options": _step_method_options(kwargs),
            "reserved_operations": _reserved_operations(kwargs),
        },
    )
    return function


def _normalize_method_options(kwargs: dict[str, object]) -> dict[str, object]:
    recycled = {"inputs", "outputs", "lane", "lanes", "in", "in_", "out"} & set(kwargs)
    if recycled:
        names = ", ".join(sorted(recycled))
        raise TypeError(f"@step method option(s) {names} were recycled; use input=..., output=..., or inout=...")
    return _normalize_transform_options(kwargs)


def _normalize_transform_options(kwargs: dict[str, object]) -> dict[str, object]:
    options = dict(kwargs)
    for name in _STEP_METHOD_OPTIONS & set(options):
        options[name] = _step_method_option(name, options[name])
    return options


def _step_method_options(kwargs: dict[str, object]) -> dict[str, object]:
    return {name: kwargs[name] for name in _STEP_METHOD_OPTIONS if name in kwargs}


def _reserved_operations(kwargs: dict[str, object]) -> tuple[OperationPlan, ...]:
    if "cache" not in kwargs:
        return ()
    return (cache_operation(kwargs["cache"]),)


def _step_method_option(name: str, value: object) -> object:
    if name in {"target_backend", "target_platform", "target_profile"}:
        if not isinstance(value, str) or not value:
            raise TypeError(f"{name} must be a non-empty string")
    return value


def _method_declarations(kwargs, *, name: str, bare: tuple[type, ...], roles: set[str]) -> tuple:
    if name not in kwargs or kwargs[name] is None:
        return ()
    return _method_declaration_values(kwargs[name], option=f"@step({name}=...)", bare=bare, roles=roles)


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
        values = _declaration_sequence(kwargs[plural], option=f"@raw({plural}=...)")
    else:
        return ()
    if not all(isinstance(value, allowed) for value in values):
        raise TypeError(f"@raw({plural}=...) requires {_declaration_kinds(allowed)} declarations")
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


def raw(
    function: Callable | None = None,
    *,
    input: object | None = None,
    output: object | None = None,
    inout: object | None = None,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    project_output: bool = False,
    streaming_safe: bool = False,
    target_backend: str | Iterable[str] | None = None,
    target_platform: str | None = None,
):
    if inout is not None and (input is not None or output is not None):
        raise TypeError("@raw(inout=...) cannot combine with input=... or output=...")
    inputs = _method_declarations(
        {"input": input},
        name="input",
        bare=(InputDeclaration, LaneDeclaration),
        roles={"input", "lane"},
    )
    outputs = _method_declarations(
        {"output": output},
        name="output",
        bare=(LaneDeclaration, OutputDeclaration),
        roles={"lane", "output"},
    )
    if inout is not None:
        binding = inout
        if not isinstance(binding, InOutBinding):
            raise TypeError("@raw(inout=...) requires a pipe binding such as source | target")
        inputs = _method_declaration_values(
            binding.inputs,
            option="@raw(inout=...) input side",
            bare=(InputDeclaration, LaneDeclaration),
            roles={"input", "lane"},
        )
        outputs = _method_declaration_values(
            binding.outputs,
            option="@raw(inout=...) output side",
            bare=(LaneDeclaration, OutputDeclaration),
            roles={"lane", "output"},
        )
    if len(set(map(_binding_key, inputs))) != len(inputs):
        raise TypeError("@raw(input=...) cannot repeat a declaration")
    if len(set(map(_binding_key, outputs))) != len(outputs):
        raise TypeError("@raw(output=...) cannot repeat a declaration")
    if target_platform is not None:
        _step_method_option("target_platform", target_platform)

    def decorate(target: Callable) -> Callable:
        setattr(
            target,
            "_structure_raw",
            {
                "inputs": inputs or None,
                "outputs": outputs or None,
                "schema_mode": schema_mode,
                "project_output": project_output,
                "streaming_safe": streaming_safe,
                "target_backend": _hook_target_backend(target_backend),
                "target_platform": target_platform,
            },
        )
        return target

    if function is None:
        return decorate
    return decorate(function)


def where(*predicates: object) -> "WhereChain":
    context = current_context()
    if context is None:
        raise RuntimeError("where(...) can only be used inside a compiled Structure step method")
    if not predicates:
        raise TypeError("where(...) requires at least one boolean Structure expression")
    for position, predicate in enumerate(predicates, start=1):
        expression = literal(predicate)
        if not isinstance(expression.type, BooleanType):
            raise TypeError(f"where(...) requires a boolean Structure expression for predicate {position}")
        if expression.kind == "existence_join" and expression.data is not None:
            join = cast(JoinPlan, expression.data["join"])
            context.joins.append(join)
            context.operations.append(OperationPlan.join_operation(join))
            continue
        context.filters.append(expression)
        context.operations.append(OperationPlan.filter_operation(expression))
    return WhereChain()


def watermark(field: object, *, delay: str = "10 minutes") -> None:
    context = current_context()
    if context is None:
        raise RuntimeError("watermark(...) can only be used inside a compiled Structure step method")
    expression = literal(field)
    if expression.kind != "field":
        raise TypeError("watermark(...) requires a Structure field expression")
    if not isinstance(delay, str) or not delay.strip():
        raise TypeError("watermark(delay=...) requires a non-empty string")
    context.operations.append(OperationPlan.watermark_operation(WatermarkPlan(expression=expression, delay=delay)))


@overload
def project(source: object, target: type[Projected]) -> Projected: ...


@overload
def project(source: object, target: Iterable[str]) -> Any: ...


@overload
def project(source: object) -> Any: ...


def project(source: object | None = None, target: type[Schema] | Iterable[str] | None = None) -> object:
    if target is None:
        context = current_context()
        if context is not None and isinstance(source, type) and issubclass(source, Schema):
            return Projection(source=_default_project_source(context), target=source)
        if context is not None and source is not None:
            return Projection(source=_default_project_source(context), fields=_project_fields(source))
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    if isinstance(source, type) and issubclass(source, Schema):
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    if source is None:
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    if isinstance(target, type):
        if not issubclass(target, Schema):
            raise TypeError("project(source, target) requires a Schema class target")
        return Projection(source=source, target=target)
    return Projection(source=source, fields=_project_fields(target))


class WhereChain:

    def where(self, *predicates: object) -> "WhereChain":
        return where(*predicates)

    @overload
    def project(self, source: type[Projected]) -> Projected: ...

    @overload
    def project(self, source: Iterable[str]) -> Any: ...

    @overload
    def project(self, source: object, target: type[Projected]) -> Projected: ...

    @overload
    def project(self, source: object, target: Iterable[str]) -> Any: ...

    def project(
        self,
        source: object | None = None,
        target: type[Schema] | Iterable[str] | None = None,
    ) -> object:
        if target is None:
            return project(source)
        return project(source, target)


def _default_project_source(context) -> object:
    if context.default_project_source is None:
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    return context.default_project_source


def _project_fields(value: object) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise TypeError("project(source, fields) requires a non-empty field sequence")
    try:
        fields = tuple(cast(Iterable[object], value))
    except TypeError as error:
        raise TypeError("project(source, fields) requires a non-empty field sequence") from error
    if not fields:
        raise TypeError("project(source, fields) requires at least one field")
    if not all(isinstance(field, str) for field in fields):
        raise TypeError("project(source, fields) requires field names as strings")
    if len(set(fields)) != len(fields):
        raise TypeError("project(source, fields) cannot repeat field names")
    return cast(tuple[str, ...], fields)


def _hook_target_backend(value: str | Iterable[str] | None) -> tuple[tuple[str, ...], bool]:
    if value is None:
        return ("pyspark",), True
    if isinstance(value, str):
        if not value:
            raise TypeError("target_backend must be a non-empty backend name")
        if value == "configured":
            return ("pyspark",), False
        return (value,), False
    if isinstance(value, bytes):
        raise TypeError("target_backend must be a backend name or a non-empty backend name sequence")
    try:
        targets = tuple(cast(Iterable[object], value))
    except TypeError as error:
        raise TypeError("target_backend must be a backend name or a non-empty backend name sequence") from error
    if not targets or not all(isinstance(target, str) and target for target in targets):
        raise TypeError("target_backend must contain non-empty backend names")
    return cast(tuple[str, ...], targets), False


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
