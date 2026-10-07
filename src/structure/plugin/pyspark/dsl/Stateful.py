"""Authoring declarations for row-based PySpark ``transformWithState``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from inspect import isfunction
from typing import Any, ClassVar, Generic, TypeVar, cast, get_args, get_origin, get_type_hints

from structure import Schema

Input = TypeVar("Input", bound=Schema)
Key = TypeVar("Key", bound=Schema)
Output = TypeVar("Output", bound=Schema)
State = TypeVar("State", bound=Schema)
MapKey = TypeVar("MapKey", bound=Schema)


class StateProcessor(Generic[Input, Key, Output]):
    """Base declaration for a typed Structure row state processor.

    State handles are declared as annotated attributes on subclasses. Callbacks
    access those typed handles through ``self``.
    """

    __structure_state_processor__: ClassVar[tuple[type[Schema], ...]]


class PandasStateProcessor(Generic[Input, Key, State, Output]):
    """Base declaration for a typed Pandas ``transformWithStateInPandas`` processor."""

    __structure_pandas_state_processor__: ClassVar[tuple[type[Schema], ...]]


class PandasGroupStateProcessor(Generic[Input, Key, State, Output]):
    """Base declaration for legacy ``applyInPandasWithState`` callbacks."""

    __structure_pandas_group_state_processor__: ClassVar[tuple[type[Schema], ...]]

    def on_batches(self, key: Key, batches: Iterator[Any], state: PandasGroupState[State]) -> Iterator[Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class ValueState(Generic[State]):
    """Typed facade for a named value state handle."""

    _handle: Any
    _schema: type[Schema]

    def exists(self) -> bool:
        return self._handle.exists()

    def get(self) -> State | None:
        value = self._handle.get()
        return None if value is None else cast(State, _schema_instance(self._schema, value))

    def update(self, value: State) -> None:
        self._handle.update(_schema_values(self._schema, value))

    def clear(self) -> None:
        self._handle.clear()


@dataclass(frozen=True)
class ListState(Generic[State]):
    """Typed facade for a named list state handle."""

    _handle: Any
    _schema: type[Schema]

    def exists(self) -> bool:
        return self._handle.exists()

    def get(self) -> Iterator[State]:
        return (cast(State, _schema_instance(self._schema, value)) for value in self._handle.get())

    def put(self, values: Iterator[State] | list[State]) -> None:
        self._handle.put([_schema_values(self._schema, value) for value in values])

    def append_value(self, value: State) -> None:
        self._handle.appendValue(_schema_values(self._schema, value))

    def append_list(self, values: Iterator[State] | list[State]) -> None:
        self._handle.appendList([_schema_values(self._schema, value) for value in values])

    def clear(self) -> None:
        self._handle.clear()


@dataclass(frozen=True)
class MapState(Generic[MapKey, State]):
    """Typed facade for a named map state handle."""

    _handle: Any
    _key_schema: type[Schema]
    _value_schema: type[Schema]

    def exists(self) -> bool:
        return self._handle.exists()

    def get_value(self, key: MapKey) -> State | None:
        value = self._handle.getValue(_schema_values(self._key_schema, key))
        return None if value is None else cast(State, _schema_instance(self._value_schema, value))

    def contains_key(self, key: MapKey) -> bool:
        return self._handle.containsKey(_schema_values(self._key_schema, key))

    def update_value(self, key: MapKey, value: State) -> None:
        self._handle.updateValue(
            _schema_values(self._key_schema, key), _schema_values(self._value_schema, value)
        )

    def iterator(self) -> Iterator[tuple[MapKey, State]]:
        return (
            (
                cast(MapKey, _schema_instance(self._key_schema, key)),
                cast(State, _schema_instance(self._value_schema, value)),
            )
            for key, value in self._handle.iterator()
        )

    def keys(self) -> Iterator[MapKey]:
        return (cast(MapKey, _schema_instance(self._key_schema, key)) for key in self._handle.keys())

    def values(self) -> Iterator[State]:
        return (cast(State, _schema_instance(self._value_schema, value)) for value in self._handle.values())

    def remove_key(self, key: MapKey) -> None:
        self._handle.removeKey(_schema_values(self._key_schema, key))

    def clear(self) -> None:
        self._handle.clear()


@dataclass(frozen=True)
class _StateDeclaration:
    kind: str
    name: str | None = None
    ttl_ms: int | None = None


@dataclass(frozen=True)
class StateAttribute:
    attribute_name: str
    name: str
    kind: str
    value_schema: type[Schema]
    key_schema: type[Schema] | None = None
    ttl_ms: int | None = None


def value_state(*, name: str | None = None, ttl: timedelta | None = None) -> Any:
    """Declare a ``ValueState`` attribute, optionally with persisted name and TTL."""

    return _state_declaration("value", name, ttl)


def list_state(*, name: str | None = None, ttl: timedelta | None = None) -> Any:
    """Declare a ``ListState`` attribute, optionally with persisted name and TTL."""

    return _state_declaration("list", name, ttl)


def map_state(*, name: str | None = None, ttl: timedelta | None = None) -> Any:
    """Declare a ``MapState`` attribute, optionally with persisted name and TTL."""

    return _state_declaration("map", name, ttl)


def _state_declaration(kind: str, name: str | None, ttl: timedelta | None) -> _StateDeclaration:
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise ValueError("State name must be a non-empty string when specified.")
    return _StateDeclaration(kind=kind, name=name, ttl_ms=_ttl_milliseconds(ttl))


def _ttl_milliseconds(ttl: timedelta | None) -> int | None:
    if ttl is None:
        return None
    if not isinstance(ttl, timedelta):
        raise TypeError("State TTL must be a datetime.timedelta.")
    microseconds = (ttl.days * 86400 + ttl.seconds) * 1_000_000 + ttl.microseconds
    if microseconds <= 0 or microseconds % 1000:
        raise ValueError("State TTL must be a positive whole number of milliseconds.")
    milliseconds = microseconds // 1000
    if milliseconds > 2**63 - 1:
        raise ValueError("State TTL exceeds the supported signed 64-bit millisecond range.")
    return milliseconds


@dataclass(frozen=True)
class PandasGroupState(Generic[State]):
    """Typed facade for legacy PySpark ``GroupState`` and its tuple value."""

    _handle: Any
    _schema: type[Schema]
    _timeout: str

    @property
    def exists(self) -> bool:
        return self._handle.exists

    def get(self) -> State | None:
        value = self._handle.getOption
        return None if value is None else cast(State, _schema_instance(self._schema, value))

    def update(self, value: State) -> None:
        self._handle.update(_schema_values(self._schema, value))

    def remove(self) -> None:
        self._handle.remove()

    @property
    def has_timed_out(self) -> bool:
        return self._handle.hasTimedOut

    def set_timeout_duration(self, duration_ms: int) -> None:
        if self._timeout != "processing_time":
            raise ValueError("set_timeout_duration(...) requires timeout='processing_time'.")
        if not isinstance(duration_ms, int) or isinstance(duration_ms, bool) or duration_ms <= 0:
            raise ValueError("set_timeout_duration(duration_ms=...) requires a positive integer number of milliseconds.")
        self._handle.setTimeoutDuration(duration_ms)

    def set_timeout_timestamp(self, timestamp_ms: int) -> None:
        if self._timeout != "event_time":
            raise ValueError("set_timeout_timestamp(...) requires timeout='event_time'.")
        if not isinstance(timestamp_ms, int) or isinstance(timestamp_ms, bool) or timestamp_ms <= 0:
            raise ValueError("set_timeout_timestamp(timestamp_ms=...) requires positive epoch milliseconds.")
        self._handle.setTimeoutTimestamp(timestamp_ms)

    @property
    def current_processing_time_ms(self) -> int:
        return self._handle.getCurrentProcessingTimeMs()

    @property
    def current_watermark_ms(self) -> int:
        return self._handle.getCurrentWatermarkMs()


@dataclass(frozen=True)
class Timer:
    timestamp_ms: int


class TimerContext:
    """Timer operations and current trigger times exposed to a typed processor."""

    def __init__(self, handle: Any, timer_values: Any | None = None) -> None:
        self._handle = handle
        self._timer_values = timer_values

    @property
    def current_processing_time_ms(self) -> int | None:
        """Return this callback's processing time, or ``None`` during initialization."""

        if self._timer_values is None:
            return None
        return self._timer_values.getCurrentProcessingTimeInMs()

    @property
    def current_watermark_ms(self) -> int | None:
        """Return this callback's watermark, or ``None`` during initialization."""

        if self._timer_values is None:
            return None
        return self._timer_values.getCurrentWatermarkInMs()

    def register(self, timestamp_ms: int) -> None:
        self._handle.registerTimer(timestamp_ms)

    def delete(self, timestamp_ms: int) -> None:
        self._handle.deleteTimer(timestamp_ms)

    def list(self) -> Iterator[Timer]:
        return (Timer(value) for value in self._handle.listTimers())


@dataclass(frozen=True)
class ExternalStateProcessor:
    processor: type
    input_schema: type[Schema]
    key_schema: type[Schema]
    state_schemas: tuple[type[Schema], ...]
    output_schema: type[Schema]


@dataclass(frozen=True)
class ExternalPandasStateFunction:
    function: Any
    input_schema: type[Schema]
    key_schema: type[Schema]
    state_schema: type[Schema]
    output_schema: type[Schema]


@dataclass(frozen=True)
class StatefulResult:
    output_schema: type[Schema]


def state_processor(processor: type[StateProcessor[Input, Key, Output]]) -> type:
    """Validate and mark a typed processor; inheritance alone is also supported."""

    setattr(processor, "_structure_special_type", "state_processor")
    return _mark_processor(
        processor,
        StateProcessor,
        "__structure_state_processor__",
        "StateProcessor",
    )


def pandas_state_processor(processor: type[PandasStateProcessor[Input, Key, State, Output]]) -> type:
    """Mark a top-level Pandas state processor class for Structure compilation."""

    return _mark_processor(
        processor,
        PandasStateProcessor,
        "__structure_pandas_state_processor__",
        "PandasStateProcessor",
    )


def pandas_group_state_processor(
    processor: type[PandasGroupStateProcessor[Input, Key, State, Output]],
) -> type:
    """Mark a top-level typed legacy Pandas state processor class."""

    return _mark_processor(
        processor,
        PandasGroupStateProcessor,
        "__structure_pandas_group_state_processor__",
        "PandasGroupStateProcessor",
    )


def external_state_processor(
    processor: type,
    *,
    input: type[Schema],
    key: type[Schema],
    states: tuple[type[Schema], ...],
    output: type[Schema],
) -> ExternalStateProcessor:
    """Bind a native PySpark processor to Structure's schema boundary."""

    schemas = (input, key, *states, output)
    if not all(isinstance(schema, type) and issubclass(schema, Schema) for schema in schemas):
        raise TypeError("external_state_processor schema bindings must be Structure Schema classes.")
    if not isinstance(states, tuple):
        raise TypeError("external_state_processor(states=...) must be a tuple of Structure Schema classes.")
    return ExternalStateProcessor(processor, input, key, states, output)


def external_pandas_state_function(
    function: Any,
    *,
    input: type[Schema],
    key: type[Schema],
    state: type[Schema],
    output: type[Schema],
) -> ExternalPandasStateFunction:
    """Bind a native ``applyInPandasWithState`` function to Structure schemas."""

    schemas = (input, key, state, output)
    if not isfunction(function):
        raise TypeError("external_pandas_state_function requires a top-level Python function.")
    if not all(isinstance(schema, type) and issubclass(schema, Schema) for schema in schemas):
        raise TypeError("external_pandas_state_function schema bindings must be Structure Schema classes.")
    _require_importable(function)
    return ExternalPandasStateFunction(function, input, key, state, output)


def _mark_processor(processor: type, origin: type, attribute: str, label: str) -> type:
    arguments = _processor_schemas(processor, origin)
    expected = 3 if origin is StateProcessor else 4
    if len(arguments) != expected or not all(isinstance(argument, type) and issubclass(argument, Schema) for argument in arguments):
        generics = "InputSchema, KeySchema, OutputSchema" if expected == 3 else "InputSchema, KeySchema, StateSchema, OutputSchema"
        raise TypeError(
            f"Processor declaration requires {processor.__name__} to inherit "
            f"{label}[{generics}]."
        )
    _require_importable(processor)
    setattr(processor, attribute, arguments)
    if origin is StateProcessor:
        declarations = _state_attributes(processor)
        _validate_typed_callbacks(processor, declarations)
        setattr(processor, "__structure_state_attributes__", declarations)
        if hasattr(processor, "on_initial_state"):
            setattr(processor, "__structure_initial_schema__", _initial_schema(processor))
    return processor


def _state_attributes(processor: type) -> tuple[StateAttribute, ...]:
    declarations: dict[str, tuple[object, object]] = {}
    bindings = _generic_bindings_by_class(processor)
    for owner in reversed(processor.__mro__):
        if owner is object:
            continue
        local_annotations = vars(owner).get("__annotations__", {})
        if not local_annotations:
            continue
        try:
            resolved = get_type_hints(owner, include_extras=True)
        except (NameError, TypeError) as error:
            raise TypeError(f"Cannot resolve state annotations on {owner.__name__}: {error}.") from error
        for attribute_name in local_annotations:
            annotation = _resolve_annotation(
                resolved.get(attribute_name, local_annotations[attribute_name]), bindings.get(owner, {})
            )
            declaration = vars(owner).get(attribute_name)
            declarations[attribute_name] = (annotation, declaration)

    attributes: list[StateAttribute] = []
    persisted_names: set[str] = set()
    reserved = {"init", "on_rows", "on_timer", "on_initial_state"}
    for attribute_name, (annotation, declaration) in declarations.items():
        origin = get_origin(annotation)
        arguments = get_args(annotation)
        kinds = {ValueState: "value", ListState: "list", MapState: "map"}
        if origin not in kinds:
            if isinstance(declaration, _StateDeclaration):
                raise TypeError(f"State factory for {attribute_name!r} requires a ValueState, ListState, or MapState annotation.")
            if annotation in kinds:
                raise TypeError(f"State attribute {attribute_name!r} must parameterize its state wrapper with concrete Schema types.")
            continue
        if attribute_name in reserved:
            raise TypeError(f"State attribute {attribute_name!r} conflicts with a processor callback or Spark lifecycle method.")
        kind = kinds[origin]
        expected_arguments = 2 if origin is MapState else 1
        if len(arguments) != expected_arguments:
            raise TypeError(f"State attribute {attribute_name!r} must use a concrete {origin.__name__} schema parameterization.")
        if origin is MapState:
            key_schema, value_schema = arguments
            if not _is_schema(key_schema) or not _is_schema(value_schema):
                raise TypeError(f"MapState attribute {attribute_name!r} requires concrete Structure Schema key and value types.")
        else:
            key_schema = None
            value_schema = arguments[0]
            if not _is_schema(value_schema):
                raise TypeError(f"{origin.__name__} attribute {attribute_name!r} requires a concrete Structure Schema type.")
        factory = declaration if isinstance(declaration, _StateDeclaration) else _StateDeclaration(kind=kind)
        if declaration is not None and not isinstance(declaration, _StateDeclaration):
            raise TypeError(f"State attribute {attribute_name!r} uses an unsupported declaration value; use a state factory.")
        if factory.kind != kind:
            raise TypeError(
                f"State factory kind {factory.kind!r} does not match {origin.__name__} annotation on {attribute_name!r}."
            )
        state_name = factory.name or attribute_name
        if state_name in persisted_names:
            raise TypeError(f"State name {state_name!r} is declared more than once; state names must be unique.")
        persisted_names.add(state_name)
        attributes.append(StateAttribute(attribute_name, state_name, kind, cast(type[Schema], value_schema), cast(type[Schema] | None, key_schema), factory.ttl_ms))
    return tuple(attributes)


def _generic_bindings_by_class(processor: type) -> dict[type, dict[TypeVar, object]]:
    result: dict[type, dict[TypeVar, object]] = {}

    def visit(current: type, bindings: dict[TypeVar, object], ancestry: frozenset[type]) -> None:
        if current in ancestry:
            return
        result[current] = bindings
        aliases = {get_origin(base) or base: base for base in vars(current).get("__orig_bases__", ())}
        for base_class in current.__bases__:
            if base_class is object:
                continue
            alias = aliases.get(base_class)
            raw_arguments = get_args(alias) if alias is not None else ()
            arguments = tuple(_resolve_typevar(argument, bindings) for argument in raw_arguments)
            parameters = getattr(base_class, "__parameters__", ())
            if alias is None:
                arguments = tuple(bindings.get(parameter, parameter) for parameter in parameters)
            visit(
                base_class,
                {parameter: argument for parameter, argument in zip(parameters, arguments, strict=False)},
                ancestry | {current},
            )

    visit(processor, {}, frozenset())
    return result


def _resolve_annotation(annotation: object, bindings: dict[TypeVar, object]) -> object:
    if isinstance(annotation, TypeVar):
        return _resolve_typevar(annotation, bindings)
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    arguments = tuple(_resolve_annotation(argument, bindings) for argument in get_args(annotation))
    try:
        return origin[arguments[0] if len(arguments) == 1 else arguments]
    except TypeError:
        return annotation


def _is_schema(value: object) -> bool:
    return isinstance(value, type) and issubclass(value, Schema)


def _validate_typed_callbacks(processor: type, declarations: tuple[StateAttribute, ...]) -> None:
    callbacks = (("on_rows", True, 4), ("on_timer", False, 4), ("on_initial_state", False, 4))
    for name, required, arity in callbacks:
        callback = getattr(processor, name, None)
        if callback is None:
            if required:
                raise TypeError("Typed StateProcessor must define on_rows(self, key, rows, timers).")
            continue
        if not callable(callback):
            raise TypeError(f"Typed StateProcessor callback {name!r} must be callable.")
        try:
            signature = __import__("inspect").signature(callback)
            signature.bind(*([object()] * arity))
        except (TypeError, ValueError) as error:
            shapes = {
                "on_rows": "(self, key, rows, timers)",
                "on_timer": "(self, key, timer, timers)",
                "on_initial_state": "(self, key, initial, timers)",
            }
            raise TypeError(f"StateProcessor.{name} must accept {shapes[name]}; received {callback!r}.") from error
    if hasattr(processor, "on_initial_state") and not callable(getattr(processor, "on_initial_state")):
        raise TypeError("StateProcessor.on_initial_state must be callable when defined.")


def _processor_schemas(processor: type, origin: type) -> tuple[object, ...]:
    """Resolve a processor's schema arguments through specialized generic bases."""
    resolved: list[tuple[object, ...]] = []

    def visit(current: type, bindings: dict[TypeVar, object], ancestry: frozenset[type]) -> None:
        if current in ancestry:
            return
        next_ancestry = ancestry | {current}
        declared_bases = vars(current).get("__orig_bases__", ())
        aliases = {get_origin(base) or base: base for base in declared_bases}
        for base_class in current.__bases__:
            if base_class is object:
                continue
            alias = aliases.get(base_class)
            raw_arguments = get_args(alias) if alias is not None else ()
            arguments = tuple(_resolve_typevar(argument, bindings) for argument in raw_arguments)
            if base_class is origin:
                resolved.append(arguments)
                continue
            if not issubclass(base_class, origin):
                continue
            parameters = getattr(base_class, "__parameters__", ())
            if alias is None:
                arguments = tuple(bindings.get(parameter, parameter) for parameter in parameters)
            visit(
                base_class,
                {parameter: argument for parameter, argument in zip(parameters, arguments, strict=False)},
                next_ancestry,
            )

    visit(processor, {}, frozenset())
    unique = tuple(dict.fromkeys(resolved))
    if len(unique) > 1:
        raise TypeError(
            f"{processor.__name__} has conflicting {origin.__name__} schema specializations: {unique!r}."
        )
    return unique[0] if unique else ()


def _resolve_typevar(argument: object, bindings: dict[TypeVar, object]) -> object:
    seen: set[TypeVar] = set()
    while isinstance(argument, TypeVar) and argument in bindings and argument not in seen:
        seen.add(argument)
        argument = bindings[argument]
    return argument


def _schema_values(schema: type[Schema], value: object) -> tuple[object, ...]:
    if not isinstance(value, schema):
        raise TypeError(f"State value must be an instance of {schema.__name__}; received {type(value).__name__}.")
    values = getattr(value, "_structure_values", {})
    _validate_schema_values(schema, values)
    return tuple(values[field.name] for field in schema._structure_fields.values())


def _schema_instance(schema: type[Schema], value: object) -> Schema:
    values = value.asDict(recursive=True) if hasattr(value, "asDict") else value
    if isinstance(values, dict):
        columns = {field.column: field.name for field in schema._structure_fields.values()}
        missing = set(columns) - set(values)
        extra = set(values) - set(columns)
        if missing or extra:
            raise ValueError(
                f"Row fields for {schema.__name__} must match its Structure Schema; "
                f"missing={sorted(missing)!r}, extra={sorted(extra)!r}."
            )
        field_values = {field.name: values[field.column] for field in schema._structure_fields.values()}
    elif isinstance(values, (tuple, list)):
        field_values = dict(zip(schema._structure_fields, values, strict=True))
    else:
        raise TypeError(f"State value for {schema.__name__} must be a row or tuple.")
    _validate_schema_values(schema, field_values)
    instance = schema(**field_values)
    for name, field_value in field_values.items():
        setattr(instance, name, field_value)
    return instance


def _validate_schema_values(schema: type[Schema], values: dict[str, object]) -> None:
    expected = set(schema._structure_fields)
    actual = set(values)
    if actual != expected:
        raise ValueError(
            f"Values for {schema.__name__} must match its Structure Schema; "
            f"missing={sorted(expected - actual)!r}, extra={sorted(actual - expected)!r}."
        )
    for field in schema._structure_fields.values():
        if not field.nullable and values[field.name] is None:
            raise ValueError(f"Value for non-nullable field {field.column!r} in {schema.__name__} is None.")


def transform_with_state(
    *,
    key: object,
    processor: type[StateProcessor] | ExternalStateProcessor,
    output_mode: str,
    time_mode: str,
    event_time_column: str | None = None,
    initial_state: object | None = None,
) -> Any:
    """Capture a row-based stateful stage in the active PySpark step."""

    return _capture_stateful_transform(
        "row",
        "transform_with_state",
        StateProcessor,
        key=key,
        processor=processor,
        output_mode=output_mode,
        time_mode=time_mode,
        event_time_column=event_time_column,
        initial_state=initial_state,
    )


def transform_with_state_in_pandas(
    *,
    key: object,
    processor: type[PandasStateProcessor] | ExternalStateProcessor,
    output_mode: str,
    time_mode: str,
    event_time_column: str | None = None,
    initial_state: object | None = None,
) -> Any:
    """Capture a Pandas-batch stateful stage in the active PySpark step."""

    return _capture_stateful_transform(
        "pandas",
        "transform_with_state_in_pandas",
        PandasStateProcessor,
        key=key,
        processor=processor,
        output_mode=output_mode,
        time_mode=time_mode,
        event_time_column=event_time_column,
        initial_state=initial_state,
    )


def apply_in_pandas_with_state(
    *,
    key: object,
    processor: type[PandasGroupStateProcessor] | ExternalPandasStateFunction,
    output_mode: str,
    timeout: str,
) -> Any:
    """Capture the legacy grouped Pandas state API as one transform step."""

    from structure.plugin.pyspark.dsl.Expression import Expression
    from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
    from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import current_pyspark_context

    context = current_pyspark_context()
    if context is None:
        raise RuntimeError("apply_in_pandas_with_state(...) is available only while compiling a PySpark transform step.")
    if not isinstance(key, Expression) or key.kind != "field":
        raise TypeError("apply_in_pandas_with_state(key=...) requires one field from the current input row.")
    if output_mode not in {"Append", "Update"}:
        raise ValueError("apply_in_pandas_with_state(output_mode=...) must be 'Append' or 'Update'.")
    if timeout not in {"none", "processing_time", "event_time"}:
        raise ValueError("apply_in_pandas_with_state(timeout=...) must be 'none', 'processing_time', or 'event_time'.")

    if isinstance(processor, ExternalPandasStateFunction):
        function = processor.function
        _require_importable(function)
        input_schema = processor.input_schema
        key_schema = processor.key_schema
        state_schema = processor.state_schema
        output_schema = processor.output_schema
        processor_mode = "native"
    else:
        if not isinstance(processor, type) or not issubclass(processor, PandasGroupStateProcessor):
            raise TypeError(
                "processor must inherit "
                "PandasGroupStateProcessor[InputSchema, KeySchema, StateSchema, OutputSchema]."
            )
        schemas = _processor_schemas(processor, PandasGroupStateProcessor)
        if len(schemas) != 4 or not all(
            isinstance(schema, type) and issubclass(schema, Schema) for schema in schemas
        ):
            raise TypeError(
                "processor must inherit PandasGroupStateProcessor[InputSchema, KeySchema, StateSchema, OutputSchema] "
                "with concrete Structure Schema classes."
            )
        _require_importable(processor)
        input_schema, key_schema, state_schema, output_schema = cast(
            tuple[type[Schema], type[Schema], type[Schema], type[Schema]], schemas
        )
        processor_mode = "typed"

    input_row = context.default_project_source
    input_schema_for_step = getattr(input_row, "_structure_scope_schema", None)
    if input_schema_for_step is not input_schema:
        raise TypeError("apply_in_pandas_with_state processor input Schema must match the step's driving input Schema.")
    key_fields = tuple(key_schema._structure_fields.values())
    if len(key_fields) != 1 or key_fields[0].type != key.type:
        raise TypeError("apply_in_pandas_with_state currently requires one key Schema field matching the grouping field.")
    if context.operations:
        raise TypeError("apply_in_pandas_with_state(...) must be the only relational operation in its step.")

    context.operations.append(
        OperationPlan.apply_in_pandas_with_state_operation(
            key=key,
            processor=processor,
            processor_mode=processor_mode,
            input_schema=input_schema,
            key_schema=key_schema,
            state_schema=state_schema,
            output_schema=output_schema,
            output_mode=output_mode,
            timeout=timeout,
        )
    )
    return StatefulResult(output_schema)


def _capture_stateful_transform(
    interface: str,
    operation_name: str,
    processor_type: type,
    *,
    key: object,
    processor: type | ExternalStateProcessor,
    output_mode: str,
    time_mode: str,
    event_time_column: str | None,
    initial_state: object | None,
) -> Any:

    from structure.plugin.pyspark.dsl.Expression import Expression
    from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
    from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import current_pyspark_context

    context = current_pyspark_context()
    if context is None:
        raise RuntimeError(f"{operation_name}(...) is available only while compiling a PySpark transform step.")
    key_expressions = (key,) if isinstance(key, Expression) else key if isinstance(key, tuple) else ()
    if not key_expressions:
        raise TypeError(f"{operation_name}(key=...) requires a field or a non-empty tuple of fields.")
    if any(not isinstance(expression, Expression) or expression.kind != "field" for expression in key_expressions):
        raise TypeError(f"{operation_name}(key=...) accepts only fields from the current input row.")
    field_identities = tuple(
        ((expression.data or {}).get("scope", ""), (expression.data or {}).get("field", ""))
        for expression in key_expressions
    )
    if len(set(field_identities)) != len(field_identities):
        raise TypeError(f"{operation_name}(key=...) does not allow duplicate grouping fields.")
    allowed_output_modes = {"Append", "Update"} if interface == "row" else {"Append", "Update", "Complete"}
    if output_mode not in allowed_output_modes:
        allowed = " or ".join(sorted(allowed_output_modes))
        raise ValueError(f"{operation_name}(output_mode=...) must be {allowed}.")
    if time_mode not in {"None", "ProcessingTime", "EventTime"}:
        raise ValueError(f"{operation_name}(time_mode=...) must be None, ProcessingTime, or EventTime.")
    state_attributes: tuple[StateAttribute, ...] = ()
    if isinstance(processor, ExternalStateProcessor):
        _require_importable(processor.processor)
        input_schema, key_schema, output_schema = processor.input_schema, processor.key_schema, processor.output_schema
        mode = "native"
        state_schema = None
        state_attributes = ()
        initial_schema = None
    else:
        other_processor_type = PandasStateProcessor if interface == "row" else StateProcessor
        other_interface = other_processor_type.__name__
        if not isinstance(processor, type):
            raise TypeError(
                f"processor must inherit {processor_type.__name__}"
                "[InputSchema, KeySchema, StateSchema, OutputSchema]."
            )
        if issubclass(processor, other_processor_type):
            raise TypeError(f"{operation_name}(...) requires {processor_type.__name__}; received {other_interface}.")
        if not issubclass(processor, processor_type):
            raise TypeError(
                f"processor must inherit {processor_type.__name__}"
                "[InputSchema, KeySchema, StateSchema, OutputSchema]."
            )
        schemas = _processor_schemas(processor, processor_type)
        expected = 3 if interface == "row" else 4
        if len(schemas) != expected or not all(isinstance(argument, type) and issubclass(argument, Schema) for argument in schemas):
            generic_parameters = "InputSchema, KeySchema, OutputSchema" if interface == "row" else "InputSchema, KeySchema, StateSchema, OutputSchema"
            raise TypeError(
                f"processor must inherit {processor_type.__name__}[{generic_parameters}] "
                "with concrete Structure Schema classes."
            )
        _require_importable(processor)
        cast_schemas = tuple(cast(type[Schema], schema) for schema in schemas)
        if interface == "row":
            input_schema, key_schema, output_schema = cast_schemas
            state_schema = None
            state_attributes = cast(tuple[StateAttribute, ...], getattr(processor, "__structure_state_attributes__", _state_attributes(processor)))
            initial_schema = _initial_schema(cast(type, processor)) if hasattr(processor, "on_initial_state") else None
            _validate_typed_callbacks(cast(type, processor), state_attributes)
        else:
            input_schema, key_schema, state_schema, output_schema = cast_schemas
            state_attributes = ()
            initial_schema = None
        mode = "typed"
    if interface == "row" and mode == "typed":
        _validate_row_callback_types(cast(type, processor), input_schema, key_schema, output_schema, initial_schema)
        has_initial_callback = hasattr(processor, "on_initial_state")
        if has_initial_callback != (initial_state is not None):
            raise TypeError("Typed on_initial_state(...) and initial_state=... must be supplied together.")
        if any(attribute.ttl_ms is not None for attribute in state_attributes) and time_mode != "ProcessingTime":
            raise TypeError("State TTL requires time_mode='ProcessingTime'.")
    input_row = context.default_project_source
    input_schema_for_step = getattr(input_row, "_structure_scope_schema", None)
    if input_schema_for_step is not input_schema:
        raise TypeError(f"{operation_name} processor input Schema must match the step's driving input Schema.")
    key_fields = tuple(key_schema._structure_fields.values())
    if len(key_fields) != len(key_expressions) or any(
        field.type != expression.type for field, expression in zip(key_fields, key_expressions, strict=True)
    ):
        raise TypeError(
            f"{operation_name} key Schema fields must match the grouping expressions by count and type."
        )
    if initial_state is not None and mode != "native" and interface != "row":
        raise TypeError("initial_state is supported for typed processors only by row transform_with_state.")
    if initial_state is not None:
        from structure.plugin.pyspark.dsl.InputScope import InputScope

        if not isinstance(initial_state, InputScope):
            raise TypeError("initial_state must be a declared transform input relation.")
        initial_fields = initial_state._structure_input_schema._structure_fields
        initial_columns = {field.column for field in initial_fields.values()}
        key_columns = {field.column for field in key_schema._structure_fields.values()}
        if not key_columns.issubset(initial_columns):
            raise TypeError("initial_state input must include every field declared by the processor key Schema.")
        if interface == "row" and mode == "typed":
            relation_schema = initial_state._structure_input_schema
            if relation_schema is not initial_schema:
                raise TypeError(
                    "Typed initial_state relation Schema must match the on_initial_state initial parameter Schema."
                )
            relation_by_column = {field.column: field for field in relation_schema._structure_fields.values()}
            for key_field in key_schema._structure_fields.values():
                initial_field = relation_by_column.get(key_field.column)
                if initial_field is None or initial_field.type != key_field.type:
                    raise TypeError(
                        "Typed initial_state key fields must match the processor Key Schema by column name and type."
                    )
    if context.operations:
        raise TypeError(f"{operation_name}(...) must be the only relational operation in its step.")
    plan = OperationPlan.transform_with_state_operation(
        key=key_expressions[0] if len(key_expressions) == 1 else key_expressions,
        processor=processor,
        interface=interface,
        processor_mode=mode,
        input_schema=input_schema,
        key_schema=key_schema,
        state_schema=state_schema,
        state_attributes=state_attributes,
        initial_schema=initial_schema,
        output_schema=output_schema,
        output_mode=output_mode,
        time_mode=time_mode,
        event_time_column=event_time_column,
        initial_state=initial_state,
    )
    context.operations.append(plan)
    return StatefulResult(output_schema)


def _initial_schema(processor: type) -> type[Schema]:
    try:
        callback = getattr(processor, "on_initial_state")
        annotation = get_type_hints(callback, include_extras=True).get("initial")
    except (NameError, TypeError) as error:
        raise TypeError(f"Cannot resolve on_initial_state initial parameter annotation: {error}.") from error
    if not _is_schema(annotation):
        raise TypeError("on_initial_state(self, key, initial, timers) must annotate initial with a concrete Structure Schema.")
    return cast(type[Schema], annotation)


def _validate_row_callback_types(
    processor: type,
    input_schema: type[Schema],
    key_schema: type[Schema],
    output_schema: type[Schema],
    initial_schema: type[Schema] | None,
) -> None:
    expected: dict[str, dict[str, object]] = {
        "on_rows": {"key": key_schema, "timers": TimerContext},
        "on_timer": {"key": key_schema, "timer": Timer, "timers": TimerContext},
        "on_initial_state": {"key": key_schema, "initial": initial_schema, "timers": TimerContext},
    }
    for callback_name, parameters in expected.items():
        callback = getattr(processor, callback_name, None)
        if callback is None:
            continue
        try:
            hints = get_type_hints(callback, include_extras=True)
        except (NameError, TypeError) as error:
            raise TypeError(f"Cannot resolve {callback_name} callback annotations: {error}.") from error
        for parameter, schema in parameters.items():
            if schema is None or parameter not in hints:
                continue
            if hints[parameter] is not schema:
                raise TypeError(
                    f"{callback_name} parameter {parameter!r} must be annotated as "
                    f"{getattr(schema, '__name__', schema)!s}; "
                    f"received {hints[parameter]!r}."
                )
        if callback_name == "on_rows" and "rows" in hints:
            row_type = get_args(hints["rows"])
            if row_type and row_type[0] is not input_schema:
                raise TypeError(f"on_rows rows must be annotated with an iterator of {input_schema.__name__} values.")
        if "return" in hints:
            return_types = get_args(hints["return"])
            if return_types and return_types[0] is not output_schema:
                raise TypeError(f"{callback_name} return type must yield {output_schema.__name__} values.")


def _require_importable(processor: Any) -> None:
    if "<locals>" in processor.__qualname__ or processor.__module__ in {"__main__", "builtins"}:
        raise TypeError(
            f"State processor {processor.__qualname__!r} must be defined at module scope in an importable module."
        )
