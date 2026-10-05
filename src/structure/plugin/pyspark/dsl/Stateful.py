"""Authoring declarations for row-based PySpark ``transformWithState``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar, cast, get_args

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
    """Typed timer operations exposed to a Structure state processor."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle

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
            f"Processor decorator requires {processor.__name__} to inherit "
            f"{label}[InputSchema, KeySchema, StateSchema, OutputSchema]."
        )
    _require_importable(processor)
    setattr(processor, attribute, arguments)
    return processor


def _processor_schemas(processor: type, origin: type) -> tuple[object, ...]:
    for base in getattr(processor, "__orig_bases__", ()):
        if getattr(base, "__origin__", None) is origin:
            return get_args(base)
    return ()


def _schema_values(schema: type[Schema], value: object) -> tuple[object, ...]:
    return tuple(getattr(value, field.name) for field in schema._structure_fields.values())


def _schema_instance(schema: type[Schema], value: object) -> Schema:
    values = value.asDict(recursive=True) if hasattr(value, "asDict") else value
    if isinstance(values, dict):
        return schema(**{field.name: values[field.column] for field in schema._structure_fields.values()})
    if isinstance(values, (tuple, list)):
        return schema(**dict(zip(schema._structure_fields, values, strict=True)))
    raise TypeError(f"State value for {schema.__name__} must be a row or tuple.")


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
        "__structure_state_processor__",
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
        "__structure_pandas_state_processor__",
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
    processor_attribute: str,
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
    else:
        schemas = getattr(processor, processor_attribute, None)
        if not isinstance(schemas, tuple) or len(schemas) != 4:
            decorator = "@state_processor" if interface == "row" else "@pandas_state_processor"
            raise TypeError(f"processor must be decorated with {decorator}.")
        if not isinstance(processor, type) or not issubclass(processor, processor_type):
            other_interface = "PandasStateProcessor" if interface == "row" else "StateProcessor"
            raise TypeError(f"{operation_name}(...) requires {processor_type.__name__}; received {other_interface}.")
        _require_importable(processor)
        input_schema, key_schema, _state_schema, output_schema = schemas
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
