"""Public decorators and declaration helpers for Structure transforms."""

from __future__ import annotations

import inspect
from dataclasses import replace
from typing import Any, Callable, Iterable, cast, overload

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.BindingSelector import BindingSelector, SelectedDeclaration
from structure.core.dsl.model.transforms.InOutBinding import InOutBinding
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputBindings, OutputDeclaration
from structure.core.dsl.model.transforms.ParameterDeclaration import ParameterDeclaration
from structure.core.dsl.model.transforms.SchemaMode import SchemaMode
from structure.core.dsl.model.transforms.SpecialFunction import IgnoredCompilerCode, SpecialFunction
from structure.core.dsl.model.transforms.StageDeclaration import StageDeclaration
from structure.core.dsl.model.transforms.Transform import Transform
from structure.plugin.api.v1.model import current_symbolic_context

_CLASS_OPTIONS = {"target", "validate_intermediate", "streaming", "warn_on_udfs", "allow_stream_to_batch"}
_STEP_METHOD_OPTIONS = {"target", "target_platform", "target_profile"}
_METHOD_BINDING_OPTIONS = {"input", "output", "inout"}
_METHOD_OPTIMIZATION_OPTIONS = {"cache"}


class _Unset:
    def __repr__(self) -> str:
        return "_UNSET"


_UNSET = _Unset()


class _DefaultFalse:
    def __repr__(self) -> str:
        return "False"


_DEFAULT_STREAMING: bool = cast(bool, _DefaultFalse())


@overload
def input(value: type[Schema], *, streaming: bool = False) -> InputDeclaration:
    """Declare an external transform input from a schema class."""
    ...


@overload
def input(value: InputDeclaration | OutputDeclaration) -> BindingSelector:
    """Select an existing input or produced output for input-side binding."""
    ...


def input(
    value: type[Schema] | InputDeclaration | OutputDeclaration,
    *,
    streaming: bool = _DEFAULT_STREAMING,
) -> InputDeclaration | BindingSelector:
    """Declare or select a transform input.

    Args:
        value: Schema class for a new input declaration, or an existing input
            declaration to select for a binding.
        streaming: Whether the input is an unbounded streaming relation.

    Returns:
        An ``InputDeclaration`` for class attributes, or a ``BindingSelector``
        when selecting an existing declaration.

    Example:
        class PublishOrders(Transform):
            orders = input(Order, streaming=True)
    """
    declared = streaming is not _DEFAULT_STREAMING
    if not declared:
        streaming = False
    if not isinstance(streaming, bool):
        raise TypeError("input(streaming=...) must be a Boolean")
    if isinstance(value, (InputDeclaration, OutputDeclaration)):
        if streaming:
            raise TypeError("input(existing_input, streaming=...) is invalid; set streaming on the declaration")
        return BindingSelector("input", value)
    if not isinstance(value, type) or not issubclass(value, Schema):
        raise TypeError("input(...) requires a Schema class")
    return InputDeclaration(schema=value, streaming=streaming, streaming_declared=declared)


@overload
def output(**bindings: object) -> OutputBindings:
    """Collect named graph output source bindings."""
    ...


@overload
def output(value: type[Schema]) -> OutputDeclaration:
    """Declare a transform output from a schema class."""
    ...


@overload
def output(value: type[Schema], source: object) -> OutputDeclaration:
    """Declare a transform output bound to a composed source."""
    ...


@overload
def output(value: OutputDeclaration) -> BindingSelector:
    """Select an existing declaration for output-side step binding."""
    ...


def output(
    value: type[Schema] | OutputDeclaration | object = _UNSET,
    source: object = _UNSET,
    **bindings: object,
) -> OutputDeclaration | OutputBindings | BindingSelector:
    """Declare or select a transform output.

    Args:
        value: Schema class for a new output declaration, or an existing output
            declaration to select for a binding.
        source: Optional composed source for an explicit output binding.
        **bindings: Named output-to-stage mappings collected in one block.

    Returns:
        An ``OutputDeclaration`` for class attributes, or a ``BindingSelector``
        for binding expressions.

    Example:
        published = output(PublishedOrder)
        outputs = output(published=published_stage.published)
    """
    if bindings:
        if value is not _UNSET or source is not _UNSET:
            raise TypeError("output(named_bindings=...) cannot be combined with a schema or source")
        return OutputBindings(tuple(bindings.items()))
    if isinstance(value, OutputDeclaration):
        if source is not _UNSET:
            raise TypeError(
                "output(existing_output, source) is invalid; select existing outputs with output(existing_output)"
            )
        return BindingSelector("output", value)
    if not isinstance(value, type) or not issubclass(value, Schema):
        raise TypeError("output(...) requires a Schema class")
    declaration = OutputDeclaration(schema=value)
    if source is _UNSET:
        return declaration
    return replace(declaration, source=source)


@overload
def lane(value: type[Schema]) -> LaneDeclaration:
    """Declare an intermediate relation from a schema class."""
    ...


@overload
def lane(value: SelectedDeclaration) -> BindingSelector:
    """Select a declaration for lane-side step binding."""
    ...


def lane(value: type[Schema] | SelectedDeclaration) -> LaneDeclaration | BindingSelector:
    """Declare or select an intermediate lane.

    Args:
        value: Schema class for a new lane, or an existing declaration selected
            for lane binding.

    Returns:
        A ``LaneDeclaration`` for class attributes, or a ``BindingSelector`` for
        binding expressions.

    Example:
        enriched = lane(EnrichedOrder)
    """
    if isinstance(value, (InputDeclaration, LaneDeclaration, OutputDeclaration)):
        return BindingSelector("lane", value)
    if not isinstance(value, type) or not issubclass(value, Schema):
        raise TypeError("lane(...) requires a Schema class")
    return LaneDeclaration(schema=value)


def parameter(default: object) -> ParameterDeclaration:
    """Declare a scalar transform parameter with a default value.

    Parameters are configured through the standard transform invocation, for
    example ``ScoreBm25(k1=1.35)``. They are distinct from schema inputs and
    are captured when a composed stage is compiled.
    """

    return ParameterDeclaration(default=default)


def stage(value: Transform) -> StageDeclaration:
    """Declare a named child transform invocation for composition.

    Args:
        value: Transform invocation, not the transform class.

    Returns:
        A stage declaration whose outputs can be referenced by attribute.

    Example:
        enrich = EnrichOrders()
        published = output(PublishedOrder, enrich.published)

    The direct assignment form is canonical for authored graphs; ``stage(...)``
    remains available for compatibility.
    """
    if not isinstance(value, Transform):
        raise TypeError("stage(...) requires a Transform invocation")
    return StageDeclaration(invocation=value)


def transform(target=None, **kwargs):
    """Decorate a ``Transform`` subclass as a Structure transform.

    Args:
        target: Optional target name or class. Passing ``"pyspark"`` selects a
            default target for the class.
        **kwargs: Class-level options such as ``target``,
            ``validate_intermediate``, ``streaming``, ``warn_on_udfs``, and
            ``allow_stream_to_batch``.
            Step defaults such as ``target_platform`` may also be supplied.

    Returns:
        A class decorator, or the decorated class when used as ``@transform``.

    Example:
        @transform(target="pyspark")
        class PublishOrders(Transform):
            orders = input(Order)
            published = output(PublishedOrder)
    """
    if isinstance(target, str):
        kwargs = {**kwargs, "target": target}
        target = None

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
    """Decorate a transform method as a schema-bound step.

    Args:
        target: Optional method when used as ``@step`` without parentheses.
        **kwargs: Binding options. Use ``input=...`` and ``output=...`` or a
            pipe expression through ``inout=...``.

    Returns:
        A method decorator, or the decorated method.

    Example:
        @step(inout=orders | published)
        def publish(self, order):
            return PublishedOrder.project(order)
    """

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
    """Decorate helper logic with plugin-visible symbolic behavior.

    Args:
        function: Helper function when used without decorator parentheses.
        type: ``"expr"`` for transparent symbolic expansion, ``"udf"`` for a
            plugin UDF expression, or ``"ignore"`` for code that must stay
            outside compiler-visible logic.
        **kwargs: ``return_type`` and ``nullable`` for UDF helpers.

    Returns:
        A callable wrapper for functions, or the original class with metadata
        when used as an expression-compatible class marker.

    Example:
        @special(type="expr")
        def normalized_email(value):
            return lower(trim(value))
    """
    allowed = {"expr", "udf", "ignore"}
    if type not in allowed:
        raise TypeError(f"@special(type=...) must use one of: {', '.join(sorted(allowed))}")
    if type == "expr" and kwargs:
        unknown = ", ".join(sorted(kwargs))
        raise TypeError(f"@special(type=\"expr\") got unknown option(s): {unknown}")
    if type == "ignore" and kwargs:
        unknown = ", ".join(sorted(kwargs))
        raise TypeError(f"@special(type=\"ignore\") got unknown option(s): {unknown}")
    if type == "udf":
        unknown_options = set(kwargs) - {"return_type", "nullable"}
        if unknown_options:
            raise TypeError(f"@special(type=\"udf\") got unknown option(s): {', '.join(sorted(unknown_options))}")
        if "nullable" in kwargs and not isinstance(kwargs["nullable"], bool):
            raise TypeError("@special(type=\"udf\") nullable must be a Boolean")

    def decorate(target: Callable) -> SpecialFunction | Callable:
        if inspect.isclass(target):
            if type not in {"expr", "ignore"}:
                raise TypeError('@special can decorate classes only with type="expr" or type="ignore"')
            setattr(target, "_structure_special_type", type)
            if type == "ignore":
                _guard_ignored_class(target)
            return target
        return SpecialFunction(
            target,
            type=type,
            return_type=kwargs.get("return_type"),
            nullable=kwargs.get("nullable", True),
        )

    if function is None:
        return decorate
    return decorate(function)


def _guard_ignored_class(cls: type) -> None:
    """Reject callable access on an ignored class only during compilation."""
    original = getattr(cls, "__getattribute__", object.__getattribute__)
    if getattr(original, "_structure_ignore_guard", False):
        return

    def guarded(instance, name):
        value = original(instance, name)
        if current_symbolic_context() is not None and not name.startswith("_") and callable(value):
            raise IgnoredCompilerCode(
                f"{cls.__qualname__}.{name} is marked @special(type=\"ignore\") and cannot be used in "
                "compiler-visible logic"
            )
        return value

    setattr(guarded, "_structure_ignore_guard", True)
    setattr(cls, "__getattribute__", cast(Any, guarded))


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
        bare=(InputDeclaration, LaneDeclaration, OutputDeclaration),
        roles={"input", "lane", "output"},
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
            roles={"input", "lane", "output"},
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
    for name in _CLASS_OPTIONS & set(options):
        if name == "target":
            if not isinstance(options[name], str) or not options[name]:
                raise TypeError("target must be a non-empty string")
            continue
        if not isinstance(options[name], bool):
            raise TypeError(f"{name} must be a Boolean")
    for name in _STEP_METHOD_OPTIONS & set(options):
        options[name] = _step_method_option(name, options[name])
    return options


def _step_method_options(kwargs: dict[str, object]) -> dict[str, object]:
    return {name: kwargs[name] for name in _STEP_METHOD_OPTIONS if name in kwargs}


def _reserved_operations(kwargs: dict[str, object]) -> tuple[object, ...]:
    if "cache" not in kwargs:
        return ()
    return (("cache", kwargs["cache"]),)


def _step_method_option(name: str, value: object) -> object:
    if name in {"target", "target_platform", "target_profile"}:
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
        return isinstance(value.declaration, (InputDeclaration, OutputDeclaration))
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
        return "input(...), output(...), or lane(...)"
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
    streaming: bool = False,
    target: str | Iterable[str] | None = None,
    target_platform: str | None = None,
):
    """Decorate a target-specific raw hook method.

    Raw hooks are escape hatches for target behavior that the portable Core DSL
    cannot model directly.  For PySpark, the hook receives plugin-specific
    runtime objects according to the selected input/output binding.

    Args:
        function: Hook method when used without decorator parentheses.
        input: Single input or lane declaration consumed by the hook.
        output: Single lane or output declaration produced by the hook.
        inout: Pipe binding such as ``orders | published``.
        schema_mode: How strictly hook results must match declared schemas.
        project_output: Whether generated code should project hook output.
        streaming: Whether the hook is compatible with streaming execution.
        target: Target name or names. Defaults to PySpark when configured.
        target_platform: Optional platform qualifier.

    Returns:
        A hook decorator, or the decorated hook method.

    Example:
        @raw(inout=orders | enriched, target="pyspark")
        def enrich_with_spark(self, df):
            return df
    """
    if not isinstance(schema_mode, SchemaMode):
        raise TypeError("@raw(schema_mode=...) requires a SchemaMode value")
    for name, value in (("project_output", project_output), ("streaming", streaming)):
        if not isinstance(value, bool):
            raise TypeError(f"@raw({name}=...) requires a Boolean")
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

    def decorate(method: Callable) -> Callable:
        setattr(
            method,
            "_structure_raw",
            {
                "inputs": inputs or None,
                "outputs": outputs or None,
                "schema_mode": schema_mode,
                "project_output": project_output,
                "streaming": streaming,
                "targets": _hook_targets(target),
                "target_platform": target_platform,
            },
        )
        return method

    if function is None:
        return decorate
    return decorate(function)


def _hook_targets(value: str | Iterable[str] | None) -> tuple[tuple[str, ...], bool]:
    if value is None:
        return ("pyspark",), True
    if isinstance(value, str):
        if not value:
            raise TypeError("target must be a non-empty target name")
        if value == "configured":
            return ("pyspark",), False
        return (value,), False
    if isinstance(value, bytes):
        raise TypeError("target must be a target name or a non-empty target name sequence")
    try:
        targets = tuple(cast(Iterable[object], value))
    except TypeError as error:
        raise TypeError("target must be a target name or a non-empty target name sequence") from error
    if not targets or not all(isinstance(target, str) and target for target in targets):
        raise TypeError("target must contain non-empty target names")
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
