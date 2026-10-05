"""Runtime adapters for Structure's state processor declarations."""

from __future__ import annotations

from importlib import import_module

from structure import Schema
from structure.plugin.pyspark.dsl.Stateful import (
    ExternalStateProcessor,
    Timer,
    TimerContext,
    ValueState,
    _schema_instance,
    _schema_values,
)
from structure.plugin.pyspark.dsl.types import ArrayType, DecimalType, MapType, StructType, StructureType


def apply_stateful_transform(
    frame,
    *,
    key,
    processor,
    input_schema: type[Schema],
    key_schema: type[Schema],
    state_schema: type[Schema] | None,
    output_schema: type[Schema],
    processor_mode: str,
    output_mode: str,
    time_mode: str,
    target_profile: str,
    event_time_column: str | None = None,
    initial_state=None,
):
    """Apply Spark's profile-specific TransformWithState Python API."""

    input_schema = _resolve_type(input_schema)
    key_schema = _resolve_type(key_schema)
    state_schema = None if state_schema is None else _resolve_type(state_schema)
    output_schema = _resolve_type(output_schema)
    processor = _resolve_object(processor)
    if target_profile != ">=4.1,<4.2":
        raise RuntimeError(f"Row-based transform_with_state requires PySpark >=4.1,<4.2, not {target_profile!r}.")
    stateful_processor = _processor_instance(
        processor,
        input_schema=input_schema,
        key_schema=key_schema,
        state_schema=state_schema,
        processor_mode=processor_mode,
    )
    grouped = frame.groupBy(key)
    if initial_state is not None and hasattr(initial_state, "groupBy"):
        initial_state = initial_state.groupBy(*(field.column for field in key_schema._structure_fields.values()))
    arguments = {
        "statefulProcessor": stateful_processor,
        "outputStructType": _spark_schema(output_schema),
        "outputMode": output_mode,
        "timeMode": time_mode,
        "initialState": initial_state,
        "eventTimeColumnName": event_time_column or "",
    }
    method_name = "transformWithState"
    method = getattr(grouped, method_name, None)
    if method is None:
        raise RuntimeError(f"The installed PySpark runtime lacks {method_name} for target {target_profile!r}.")
    return method(**arguments)


def _resolve_type(value):
    return _resolve_object(value)


def _resolve_object(value):
    if not isinstance(value, str):
        return value
    module, separator, name = value.partition(":")
    if not separator:
        raise TypeError(f"Generated state processor reference {value!r} is malformed.")
    resolved = import_module(module)
    for part in name.split("."):
        resolved = getattr(resolved, part)
    return resolved


def _processor_instance(processor, *, input_schema, key_schema, state_schema, processor_mode: str):
    if isinstance(processor, ExternalStateProcessor):
        return processor.processor()
    if processor_mode == "native":
        return processor()
    if state_schema is None:
        raise TypeError("Typed state processor is missing its ValueState schema.")
    from pyspark.sql import Row
    from pyspark.sql.streaming.stateful_processor import StatefulProcessor

    class StructureStateProcessorAdapter(StatefulProcessor):
        def init(self, handle) -> None:
            self._user = processor()
            self._state = handle.getValueState("structure_value_state", _spark_schema(state_schema))
            self._timers = TimerContext(handle)

        def handleInputRows(self, key, rows, timerValues):
            values = self._user.on_rows(
                _schema_instance(key_schema, key),
                (_schema_instance(input_schema, row) for row in rows),
                ValueState(self._state, state_schema),
                self._timers,
            )
            yield from _output_rows(values, row_type=Row)

        def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
            callback = getattr(self._user, "on_timer", None)
            if callback is None:
                return
            values = callback(
                _schema_instance(key_schema, key),
                Timer(expiredTimerInfo.getExpiryTimeInMs()),
                ValueState(self._state, state_schema),
                self._timers,
            )
            yield from _output_rows(values, row_type=Row)

    return StructureStateProcessorAdapter()


def _output_rows(values, *, row_type):
    for value in values:
        mapping = _output_mapping(value)
        yield row_type(**mapping)


def _output_mapping(value):
    if isinstance(value, Schema):
        return {field.column: getattr(value, field.name) for field in value.__class__._structure_fields.values()}
    return value


def _spark_schema(schema: type[Schema]):
    from pyspark.sql import types as T

    return T.StructType(
        [T.StructField(field.column, _spark_type(field.type), field.nullable) for field in schema._structure_fields.values()]
    )


def _spark_type(type_: StructureType):
    from pyspark.sql import types as T

    primitive = {
        "string": T.StringType,
        "integer": T.IntegerType,
        "long": T.LongType,
        "float": T.FloatType,
        "double": T.DoubleType,
        "boolean": T.BooleanType,
        "binary": T.BinaryType,
        "date": T.DateType,
        "timestamp": T.TimestampType,
        "timestamp_ntz": T.TimestampNTZType,
    }
    if type_.name in primitive:
        return primitive[type_.name]()
    if isinstance(type_, DecimalType):
        return T.DecimalType(type_.precision, type_.scale)
    if isinstance(type_, ArrayType):
        return T.ArrayType(_spark_type(type_.element), containsNull=type_.contains_null)
    if isinstance(type_, MapType):
        return T.MapType(_spark_type(type_.key), _spark_type(type_.value), valueContainsNull=type_.value_contains_null)
    if isinstance(type_, StructType):
        return _spark_schema(type_.schema)
    if type_.name == "variant":
        return T.VariantType()
    raise TypeError(f"No PySpark SQL type mapping exists for Structure type {type_.name!r}.")
