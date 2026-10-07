"""Runtime adapters for Structure's state processor declarations."""

from __future__ import annotations

from importlib import import_module
from inspect import signature
from typing import Any, cast

from structure import Schema
from structure.plugin.pyspark.dsl.Stateful import (
    ExternalPandasStateFunction,
    ExternalStateProcessor,
    ListState,
    MapState,
    PandasGroupState,
    PandasGroupStateProcessor,
    StateAttribute,
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
    interface: str = "row",
    output_mode: str,
    time_mode: str,
    target_profile: str,
    event_time_column: str | None = None,
    initial_state=None,
    state_attributes: tuple[StateAttribute, ...] = (),
    initial_schema: type[Schema] | None = None,
):
    """Apply Spark's profile-specific TransformWithState Python API."""

    input_schema = _resolve_type(input_schema)
    key_schema = _resolve_type(key_schema)
    state_schema = None if state_schema is None else _resolve_type(state_schema)
    output_schema = _resolve_type(output_schema)
    processor = _resolve_object(processor)
    state_attributes = tuple(
        StateAttribute(
            attribute.attribute_name,
            attribute.name,
            attribute.kind,
            _resolve_type(attribute.value_schema),
            None if attribute.key_schema is None else _resolve_type(attribute.key_schema),
            attribute.ttl_ms,
        )
        if isinstance(attribute, StateAttribute)
        else StateAttribute(
            attribute[0],
            attribute[1],
            attribute[2],
            _resolve_type(attribute[3]),
            None if attribute[4] is None else _resolve_type(attribute[4]),
            attribute[5],
        )
        for attribute in state_attributes
    )
    initial_schema = None if initial_schema is None else _resolve_type(initial_schema)
    if interface == "row" and processor_mode == "typed":
        state_attributes = state_attributes or getattr(processor, "__structure_state_attributes__", ())
        initial_schema = initial_schema or getattr(processor, "__structure_initial_schema__", None)
    if interface not in {"row", "pandas"}:
        raise ValueError(f"Unknown state processor interface {interface!r}.")
    if interface == "row" and target_profile != ">=4.1,<4.2":
        raise RuntimeError(f"Row-based transform_with_state requires PySpark >=4.1,<4.2, not {target_profile!r}.")
    if interface == "pandas" and target_profile not in {">=4.0,<4.1", ">=4.1,<4.2"}:
        raise RuntimeError(
            "Pandas transform_with_state_in_pandas requires PySpark >=4.0,<4.1 or >=4.1,<4.2, "
            f"not {target_profile!r}."
        )
    if interface == "pandas" and output_mode == "Complete":
        raise ValueError(
            "transform_with_state_in_pandas(output_mode='Complete') is unsupported for Spark streaming output; "
            "use 'Append' or 'Update'. Spark rejects Complete for this operator before starting the query."
        )
    if interface == "pandas":
        _require_pandas_runtime()
    stateful_processor = _processor_instance(
        processor,
        input_schema=input_schema,
        key_schema=key_schema,
        state_schema=state_schema,
        output_schema=output_schema,
        processor_mode=processor_mode,
        interface=interface,
        state_attributes=state_attributes,
        initial_schema=initial_schema,
        time_mode=time_mode,
    )
    keys = key if isinstance(key, tuple) else (key,)
    grouped = frame.groupBy(*keys)
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
    method_name = "transformWithState" if interface == "row" else "transformWithStateInPandas"
    method = getattr(grouped, method_name, None)
    if method is None:
        raise RuntimeError(f"The installed PySpark runtime lacks {method_name} for target {target_profile!r}.")
    return method(**arguments)


def apply_legacy_pandas_state(
    frame,
    *,
    key,
    processor,
    input_schema: type[Schema],
    key_schema: type[Schema],
    state_schema: type[Schema],
    output_schema: type[Schema],
    processor_mode: str,
    output_mode: str,
    timeout: str,
    target_profile: str,
):
    """Apply the legacy grouped Pandas state API on an admitted profile."""

    supported_profiles = {">=3.5,<4.1", ">=3.5,<4.0", ">=4.0,<4.1", ">=4.1,<4.2"}
    if target_profile not in supported_profiles:
        raise RuntimeError(
            "apply_in_pandas_with_state requires ordinary PySpark >=3.5,<4.2 within the supported "
            f"profiles; received {target_profile!r}."
        )
    input_schema = _resolve_type(input_schema)
    key_schema = _resolve_type(key_schema)
    state_schema = _resolve_type(state_schema)
    output_schema = _resolve_type(output_schema)
    if isinstance(processor, str):
        processor = _resolve_object(processor)
    if processor_mode == "native":
        if isinstance(processor, ExternalPandasStateFunction):
            function = processor.function
        else:
            function = processor
    elif processor_mode == "typed":
        if not isinstance(processor, type) or not issubclass(processor, PandasGroupStateProcessor):
            raise TypeError("Typed apply_in_pandas_with_state processor must inherit PandasGroupStateProcessor.")
        _validate_legacy_pandas_callback(processor)

        def function(key_value, batches, group_state):
            instance = processor()
            return instance.on_batches(
                _schema_instance(key_schema, key_value),
                batches,
                PandasGroupState(group_state, state_schema, timeout),
            )

    else:
        raise ValueError(f"Unknown apply_in_pandas_with_state processor mode {processor_mode!r}.")
    _require_legacy_pandas_runtime()
    from pyspark.sql.streaming.state import GroupStateTimeout

    timeout_conf = {
        "none": GroupStateTimeout.NoTimeout,
        "processing_time": GroupStateTimeout.ProcessingTimeTimeout,
        "event_time": GroupStateTimeout.EventTimeTimeout,
    }.get(timeout)
    if timeout_conf is None:
        raise ValueError(f"Unknown apply_in_pandas_with_state timeout {timeout!r}.")
    method = getattr(frame.groupBy(key), "applyInPandasWithState", None)
    if method is None:
        raise RuntimeError(
            f"The installed PySpark runtime lacks GroupedData.applyInPandasWithState for target {target_profile!r}."
        )
    return method(
        func=function,
        outputStructType=_spark_schema(output_schema),
        stateStructType=_spark_schema(state_schema),
        outputMode=output_mode,
        timeoutConf=timeout_conf,
    )


def _validate_legacy_pandas_callback(processor: type) -> None:
    callback = getattr(processor, "on_batches", None)
    if not callable(callback):
        raise TypeError("Typed PandasGroupStateProcessor must define callable on_batches(self, key, batches, state).")
    try:
        callback_signature = signature(callback)
    except (TypeError, ValueError) as error:
        raise TypeError("Cannot inspect typed PandasGroupStateProcessor callback 'on_batches'.") from error
    try:
        callback_signature.bind(*([object()] * 4))
    except TypeError as error:
        raise TypeError(
            "Typed PandasGroupStateProcessor.on_batches must accept (self, key, batches, state); "
            f"received signature {callback_signature}."
        ) from error


def _require_legacy_pandas_runtime() -> None:
    required = ("pandas", "pyarrow")
    missing = []
    for module in required:
        try:
            import_module(module)
        except ImportError:
            missing.append(module)
    if missing:
        raise RuntimeError(
            "apply_in_pandas_with_state requires pandas and pyarrow on the driver and every worker; "
            f"the driver is missing: {', '.join(missing)}. Install compatible versions in the Spark runtime. "
            "See docs/reference/Streaming.ref.md."
        )


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


def _processor_instance(
    processor,
    *,
    input_schema,
    key_schema,
    state_schema,
    output_schema,
    processor_mode: str,
    interface: str,
    state_attributes: tuple[StateAttribute, ...] = (),
    initial_schema: type[Schema] | None = None,
    time_mode: str = "None",
):
    if isinstance(processor, ExternalStateProcessor):
        return processor.processor()
    if processor_mode == "native":
        return processor()
    if interface == "row":
        return _row_processor_instance(
            processor,
            input_schema=input_schema,
            key_schema=key_schema,
            output_schema=output_schema,
            state_attributes=state_attributes,
            initial_schema=initial_schema,
            time_mode=time_mode,
        )
    if state_schema is None:
        raise TypeError("Typed Pandas state processor is missing its state schema.")
    _validate_typed_callbacks(processor, interface)
    from pyspark.sql import Row
    from pyspark.sql.streaming.stateful_processor import StatefulProcessor

    class StructureStateProcessorAdapter(StatefulProcessor):
        def init(self, handle) -> None:
            self._user = processor()
            self._handle = handle
            self._state = handle.getValueState("structure_value_state", _spark_schema(state_schema))

        def handleInputRows(self, key, rows, timerValues):
            wrapped_key = _schema_instance(key_schema, key)
            value_state: ValueState[Schema] = ValueState(self._state, state_schema)
            timers = TimerContext(self._handle, timerValues)
            yield from _output_pandas_frames(
                self._user.on_batches(wrapped_key, rows, value_state, timers),
                output_schema,
                callback_name="on_batches",
            )

        def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
            callback = getattr(self._user, "on_timer", None)
            if callback is None:
                return
            timers = TimerContext(self._handle, timerValues)
            values = callback(
                _schema_instance(key_schema, key),
                Timer(expiredTimerInfo.getExpiryTimeInMs()),
                ValueState(self._state, state_schema),
                timers,
            )
            yield from _output_pandas_frames(values, output_schema, callback_name="on_timer")

    return StructureStateProcessorAdapter()


def _row_processor_instance(
    processor,
    *,
    input_schema,
    key_schema,
    output_schema,
    state_attributes: tuple[StateAttribute, ...],
    initial_schema: type[Schema] | None,
    time_mode: str,
):
    _validate_typed_callbacks(processor, "row")
    from pyspark.sql import Row
    from pyspark.sql.streaming.stateful_processor import StatefulProcessor

    class StructureStateProcessorAdapter(StatefulProcessor):
        def init(self, handle) -> None:
            self._handle = handle
            self._user = processor()
            for attribute in state_attributes:
                ttl = attribute.ttl_ms
                if ttl is not None and time_mode != "ProcessingTime":
                    raise ValueError("State TTL requires time_mode='ProcessingTime'.")
                if attribute.kind == "value":
                    spark_handle = _create_state_handle(handle, "getValueState", attribute, attribute.value_schema)
                    wrapper: Any = ValueState(spark_handle, attribute.value_schema)
                elif attribute.kind == "list":
                    spark_handle = _create_state_handle(handle, "getListState", attribute, attribute.value_schema)
                    wrapper = ListState(spark_handle, attribute.value_schema)
                else:
                    assert attribute.key_schema is not None
                    spark_handle = handle.getMapState(
                        attribute.name,
                        _spark_schema(attribute.key_schema),
                        _spark_schema(attribute.value_schema),
                        **({} if ttl is None else {"ttlDurationMs": ttl}),
                    )
                    wrapper = MapState(spark_handle, attribute.key_schema, attribute.value_schema)
                setattr(self._user, attribute.attribute_name, wrapper)

        def handleInputRows(self, key, rows, timerValues):
            values = self._user.on_rows(
                _schema_instance(key_schema, key),
                (_schema_instance(input_schema, row) for row in rows),
                TimerContext(self._handle, timerValues),
            )
            yield from _output_rows(values, output_schema=output_schema, row_type=Row)

        def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
            callback = getattr(self._user, "on_timer", None)
            if callback is None:
                return
            values = callback(
                _schema_instance(key_schema, key),
                Timer(expiredTimerInfo.getExpiryTimeInMs()),
                TimerContext(self._handle, timerValues),
            )
            if values is not None:
                yield from _output_rows(values, output_schema=output_schema, row_type=Row)

        def handleInitialState(self, key, initial_rows, timerValues):
            callback = getattr(self._user, "on_initial_state", None)
            if callback is None:
                return
            if initial_schema is None:
                raise TypeError("Typed initial-state callback is missing its Structure Schema.")
            callback(
                _schema_instance(key_schema, key),
                _schema_instance(initial_schema, initial_rows),
                TimerContext(self._handle, timerValues),
            )

    return StructureStateProcessorAdapter()


def _create_state_handle(handle, method_name: str, attribute: StateAttribute, schema: type[Schema]):
    method = getattr(handle, method_name)
    options = {} if attribute.ttl_ms is None else {"ttlDurationMs": attribute.ttl_ms}
    return method(attribute.name, _spark_schema(schema), **options)


def _validate_typed_callbacks(processor: type, interface: str) -> None:
    if interface == "row":
        # The decorator performs the same check during compilation. Keep this
        # runtime guard for generated modules whose class may have changed.
        from structure.plugin.pyspark.dsl.Stateful import _validate_typed_callbacks as validate

        validate(processor, cast(tuple[StateAttribute, ...], getattr(processor, "__structure_state_attributes__", ())))
        return
    input_callback = "on_rows" if interface == "row" else "on_batches"
    callbacks = ((input_callback, True), ("on_timer", False))
    for name, required in callbacks:
        callback = getattr(processor, name, None)
        if callback is None and not required:
            continue
        if not callable(callback):
            requirement = "required" if required else "optional when defined"
            raise TypeError(f"Typed state processor callback {name!r} is {requirement} and must be callable.")
        try:
            callback_signature = signature(callback)
        except (TypeError, ValueError) as error:
            raise TypeError(f"Cannot inspect typed state processor callback {name!r}.") from error
        try:
            callback_signature.bind(*([object()] * 5))
        except TypeError as error:
            expected = "(self, key, rows, state, timers)" if name == "on_rows" else None
            if name == "on_batches":
                expected = "(self, key, batches, state, timers)"
            elif name == "on_timer":
                expected = "(self, key, timer, state, timers)"
            raise TypeError(
                f"Typed state processor callback {name!r} must accept {expected}; "
                f"received signature {callback_signature}."
            ) from error


def _output_rows(values, *, output_schema: type[Schema], row_type):
    try:
        iterator = iter(values)
    except TypeError as error:
        raise TypeError(f"Typed state processor must return an iterable of {output_schema.__name__} values.") from error

    fields = tuple(output_schema._structure_fields.values())
    expected = [field.column for field in fields]
    for value in iterator:
        if not isinstance(value, output_schema):
            raise TypeError(
                f"Typed state processor must yield {output_schema.__name__} values; received {type(value).__name__}."
            )
        mapping = _output_mapping(value)
        actual = list(mapping)
        if actual != expected:
            raise ValueError(
                "Typed state processor output fields must match the declared Structure output Schema: "
                f"expected {expected!r}, received {actual!r}."
            )
        for field in fields:
            if not field.nullable and mapping[field.column] is None:
                raise ValueError(
                    f"Typed state processor output field {field.column!r} is non-nullable but received None."
                )
        yield row_type(**mapping)


def _output_pandas_frames(values, output_schema: type[Schema], *, callback_name: str):
    import pandas as pd  # type: ignore[import-untyped]

    expected = [field.column for field in output_schema._structure_fields.values()]
    for frame in values:
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(
                f"Pandas state processor callback {callback_name!r} must yield pandas.DataFrame values; "
                f"received {type(frame).__name__}. See Troubleshooting.md#typed-pandas-state-output-or-dependency-error."
            )
        if frame.empty and len(frame.columns) == 0:
            yield pd.DataFrame(columns=expected)
            continue
        actual = list(frame.columns)
        if actual != expected:
            raise ValueError(
                f"Pandas state processor callback {callback_name!r} output columns must match the declared "
                f"Structure output Schema: expected {expected!r}, received {actual!r}. "
                "See Troubleshooting.md#typed-pandas-state-output-or-dependency-error."
            )
        yield frame


def _require_pandas_runtime() -> None:
    required = ("pandas", "pyarrow", "google.protobuf")
    missing = []
    for module in required:
        try:
            import_module(module)
        except ImportError:
            missing.append(module)
    if missing:
        raise RuntimeError(
            "transform_with_state_in_pandas requires pandas, pyarrow, and protobuf on the driver and every worker; "
            f"the driver is missing: {', '.join(missing)}. Install compatible versions in the Spark runtime. "
            "See Troubleshooting.md#typed-pandas-state-output-or-dependency-error."
        )


def _output_mapping(value):
    if isinstance(value, Schema):
        values = getattr(value, "_structure_values", {})
        return {field.column: values[field.name] for field in value.__class__._structure_fields.values()}
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
