"""Authoring declarations for row-based PySpark ``transformWithState``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar, cast, get_args, get_origin

from structure import Schema

Input = TypeVar("Input", bound=Schema)
Key = TypeVar("Key", bound=Schema)
State = TypeVar("State", bound=Schema)
Output = TypeVar("Output", bound=Schema)


class StateProcessor(Generic[Input, Key, State, Output]):
    """Base declaration for a typed Structure state processor.

    Subclasses implement ``on_rows`` and may implement ``on_timer``. Their
    callback bodies are ordinary Python and run on Spark workers.
    """

    __structure_state_processor__: ClassVar[tuple[type[Schema], ...]]


class PandasStateProcessor(Generic[Input, Key, State, Output]):
    """Base declaration for a typed Pandas ``transformWithStateInPandas`` processor."""

    __structure_pandas_state_processor__: ClassVar[tuple[type[Schema], ...]]


@dataclass(frozen=True)
class ValueState(Generic[State]):
    """Typed facade for the processor's single declared value state."""

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
class StatefulResult:
    output_schema: type[Schema]


def state_processor(processor: type[StateProcessor[Input, Key, State, Output]]) -> type:
    """Mark a top-level typed processor class for Structure compilation."""

    return _mark_processor(processor, StateProcessor, "__structure_state_processor__", "StateProcessor")


def pandas_state_processor(processor: type[PandasStateProcessor[Input, Key, State, Output]]) -> type:
    """Mark a top-level Pandas state processor class for Structure compilation."""

    return _mark_processor(
        processor,
        PandasStateProcessor,
        "__structure_pandas_state_processor__",
        "PandasStateProcessor",
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


def _mark_processor(processor: type, origin: type, attribute: str, label: str) -> type:
    arguments = _processor_schemas(processor, origin)
    if len(arguments) != 4 or not all(isinstance(argument, type) and issubclass(argument, Schema) for argument in arguments):
        raise TypeError(
            f"Processor declaration requires {processor.__name__} to inherit "
            f"{label}[InputSchema, KeySchema, StateSchema, OutputSchema]."
        )
    _require_importable(processor)
    setattr(processor, attribute, arguments)
    return processor


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
    if not isinstance(key, Expression) or key.kind != "field":
        raise TypeError(f"{operation_name}(key=...) requires one field from the current input row.")
    if output_mode not in {"Append", "Update", "Complete"}:
        raise ValueError(f"{operation_name}(output_mode=...) must be Append, Update, or Complete.")
    if time_mode not in {"None", "ProcessingTime", "EventTime"}:
        raise ValueError(f"{operation_name}(time_mode=...) must be None, ProcessingTime, or EventTime.")
    if isinstance(processor, ExternalStateProcessor):
        _require_importable(processor.processor)
        input_schema, key_schema, output_schema = processor.input_schema, processor.key_schema, processor.output_schema
        mode = "native"
        state_schema = None
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
        if len(schemas) != 4 or not all(
            isinstance(argument, type) and issubclass(argument, Schema) for argument in schemas
        ):
            raise TypeError(
                f"processor must inherit {processor_type.__name__}"
                "[InputSchema, KeySchema, StateSchema, OutputSchema] with concrete Structure Schema classes."
            )
        _require_importable(processor)
        input_schema, key_schema, state_schema, output_schema = (
            cast(type[Schema], schema) for schema in schemas
        )
        mode = "typed"
    input_row = context.default_project_source
    input_schema_for_step = getattr(input_row, "_structure_scope_schema", None)
    if input_schema_for_step is not input_schema:
        raise TypeError(f"{operation_name} processor input Schema must match the step's driving input Schema.")
    key_fields = tuple(key_schema._structure_fields.values())
    if len(key_fields) != 1 or key_fields[0].type != key.type:
        raise TypeError(f"{operation_name} currently requires one key Schema field matching the grouping expression.")
    if initial_state is not None and mode != "native":
        raise TypeError("initial_state is available only with external_state_processor(...).")
    if initial_state is not None:
        from structure.plugin.pyspark.dsl.InputScope import InputScope

        if not isinstance(initial_state, InputScope):
            raise TypeError("initial_state must be a declared transform input relation.")
        initial_fields = initial_state._structure_input_schema._structure_fields
        if not set(key_schema._structure_fields).issubset(initial_fields):
            raise TypeError("initial_state input must include every field declared by the processor key Schema.")
    if context.operations:
        raise TypeError(f"{operation_name}(...) must be the only relational operation in its step.")
    plan = OperationPlan.transform_with_state_operation(
        key=key,
        processor=processor,
        interface=interface,
        processor_mode=mode,
        input_schema=input_schema,
        key_schema=key_schema,
        state_schema=state_schema,
        output_schema=output_schema,
        output_mode=output_mode,
        time_mode=time_mode,
        event_time_column=event_time_column,
        initial_state=initial_state,
    )
    context.operations.append(plan)
    return StatefulResult(output_schema)


def _require_importable(processor: type) -> None:
    if "<locals>" in processor.__qualname__ or processor.__module__ in {"__main__", "builtins"}:
        raise TypeError(
            f"State processor {processor.__qualname__!r} must be defined at module scope in an importable module."
        )
