from __future__ import annotations

import sys
from pathlib import Path
from typing import Generic, TypeVar, cast

import pytest

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from structure import Schema, Transform, input, output, step
from structure.core.compiler.api import Compiler
from structure.core.compiler.artifacts.commands.BuildArtifactFingerprint import BuildArtifactFingerprint
from structure.core.compiler.diagnostics.model.StructureCompileError import StructureCompileError
from structure.plugin.api.v1.model import BackendCapabilityError, CapabilityRequirement
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.field import string
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.Stateful import (
    ExternalStateProcessor,
    StateProcessor,
    TimerContext,
    _schema_instance,
    _schema_values,
    external_state_processor,
    state_processor,
    transform_with_state,
)
from structure.plugin.pyspark.execution.stateful import _output_rows, _validate_typed_callbacks


class Input(Schema):
    customer_id = string(nullable=False)


class Key(Schema):
    customer_id = string(nullable=False)


class OtherKey(Schema):
    customer_id = string(nullable=False)


class State(Schema):
    total = string(nullable=False)


class Output(Schema):
    total = string(nullable=False)


class CompositeInput(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)


class CompositeKey(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)


class CompositeState(Schema):
    total = string(nullable=False)


class CompositeOutput(Schema):
    total = string(nullable=False)


@state_processor
class CompositeCounter(StateProcessor[CompositeInput, CompositeKey, CompositeState, CompositeOutput]):
    pass


def test_input_relation_fingerprints_are_stable_across_scope_instances() -> None:
    fingerprint = BuildArtifactFingerprint()
    left = InputScope(name="initial", schema=Input)
    right = InputScope(name="initial", schema=Input)
    other = InputScope(name="other", schema=Input)

    assert fingerprint(left) == fingerprint(right)
    assert fingerprint(left) != fingerprint(other)


InputSchema = TypeVar("InputSchema", bound=Schema)
KeySchema = TypeVar("KeySchema", bound=Schema)
StateSchema = TypeVar("StateSchema", bound=Schema)
OutputSchema = TypeVar("OutputSchema", bound=Schema)


class GenericCounter(StateProcessor[InputSchema, KeySchema, StateSchema, OutputSchema], Generic[
    InputSchema, KeySchema, StateSchema, OutputSchema
]):
    pass


class Counter(GenericCounter[Input, Key, State, Output]):
    pass


class UnresolvedCounter(GenericCounter):
    pass


@state_processor
class DecoratedCounter(StateProcessor[Input, Key, State, Output]):
    pass


class StreamingTotals(Transform):
    events = input(Input, streaming=True)
    output_schema = output(Output)

    @step(input=events, output=output_schema)
    def calculate(self, row: Input) -> Output:
        return transform_with_state(
            key=row.customer_id,
            processor=Counter,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


def test_typed_processor_schema_hints_are_the_source_of_truth() -> None:
    from structure.plugin.pyspark.dsl.Stateful import _processor_schemas

    assert _processor_schemas(Counter, StateProcessor) == (Input, Key, State, Output)
    assert DecoratedCounter.__structure_state_processor__ == (Input, Key, State, Output)


def test_transform_with_state_lowers_for_spark_4_1() -> None:
    profile = ">=4.1,<4.2"
    compiled = Compiler.frontend.compile()(
        StreamingTotals,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": profile, "variant": "ordinary"}},
    )
    lowered = cast(PySparkExecutionPlan, compiled.lowered).steps[0].operations[0].stateful_transform
    assert lowered is not None

    assert lowered.processor_mode == "typed"
    assert lowered.key_schema is Key
    assert lowered.state_schema is State
    assert lowered.output_schema is Output
    assert lowered.output_mode == "Update"

    generated_modules = PySpark.render.project()(
        cast(PySparkExecutionPlan, compiled.lowered),
        source_transform=f"{__name__}.StreamingTotals",
        generated_package="stateful_generated",
        source_schema_modules={__name__: [Input, Output]},
    )
    generated = "\n".join(generated_modules.values())
    assert "apply_stateful_transform(" in generated
    assert f"{__name__}:Counter" in generated
    for module in generated_modules.values():
        compile(module, "<generated-transform-with-state>", "exec")


def test_row_transform_with_state_is_rejected_for_spark_4_0() -> None:
    with pytest.raises(BackendCapabilityError):
        Compiler.frontend.compile()(
            StreamingTotals,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
        )


def test_row_transform_with_state_rejects_complete_output_mode() -> None:
    class CompleteOutput(Transform):
        events = input(Input, streaming=True)
        output_schema = output(Output)

        @step(input=events, output=output_schema)
        def calculate(self, row: Input) -> Output:
            return transform_with_state(
                key=row.customer_id,
                processor=Counter,
                output_mode="Complete",
                time_mode="None",
            )

    with pytest.raises(StructureCompileError, match="must be Append or Update"):
        Compiler.frontend.compile()(
            CompleteOutput,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
        )


def test_typed_processor_requires_explicit_schema_type_parameters() -> None:
    with pytest.raises(TypeError, match=r"StateProcessor\[InputSchema, KeySchema"):

        class MissingSchemas(StateProcessor):
            pass

        class UsesMissingSchemas(Transform):
            events = input(Input, streaming=True)
            output_schema = output(Output)

            @step(input=events, output=output_schema)
            def calculate(self, row: Input) -> Output:
                return transform_with_state(
                    key=row.customer_id,
                    processor=MissingSchemas,
                    output_mode="Update",
                    time_mode="ProcessingTime",
                )

        Compiler.frontend.compile()(
            UsesMissingSchemas,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
        )


def test_typed_processor_rejects_unresolved_inherited_type_variables() -> None:
    class UnresolvedStreamingTotals(Transform):
        events = input(Input, streaming=True)
        output_schema = output(Output)

        @step(input=events, output=output_schema)
        def calculate(self, row: Input) -> Output:
            return transform_with_state(
                key=row.customer_id,
                processor=UnresolvedCounter,
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    with pytest.raises(TypeError, match="concrete Structure Schema classes"):
        Compiler.frontend.compile()(
            UnresolvedStreamingTotals,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
        )


def test_processor_rejects_conflicting_generic_specializations() -> None:
    class LeftCounter(StateProcessor[Input, Key, State, Output]):
        pass

    class RightCounter(StateProcessor[Input, OtherKey, State, Output]):
        pass

    class ConflictingCounter(LeftCounter, RightCounter):
        pass

    with pytest.raises(TypeError, match="conflicting StateProcessor schema specializations"):
        from structure.plugin.pyspark.dsl.Stateful import _processor_schemas

        _processor_schemas(ConflictingCounter, StateProcessor)


def test_external_processor_requires_explicit_schema_bindings() -> None:
    class NativeProcessor:
        pass

    binding = external_state_processor(
        NativeProcessor,
        input=Input,
        key=Key,
        states=(State,),
        output=Output,
    )

    assert binding == ExternalStateProcessor(NativeProcessor, Input, Key, (State,), Output)


def test_timer_context_exposes_callback_times_and_preserves_timer_operations() -> None:
    class Handle:
        registered: list[int] = []
        deleted: list[int] = []

        def registerTimer(self, timestamp: int) -> None:
            self.registered.append(timestamp)

        def deleteTimer(self, timestamp: int) -> None:
            self.deleted.append(timestamp)

        def listTimers(self) -> list[int]:
            return [20, 30]

    class TimerValues:
        def getCurrentProcessingTimeInMs(self) -> int:
            return 10

        def getCurrentWatermarkInMs(self) -> int:
            return 5

    handle = Handle()
    timers = TimerContext(handle, TimerValues())
    timers.register(40)
    timers.delete(20)

    assert timers.current_processing_time_ms == 10
    assert timers.current_watermark_ms == 5
    assert [timer.timestamp_ms for timer in timers.list()] == [20, 30]
    assert handle.registered == [40]
    assert handle.deleted == [20]
    assert TimerContext(handle).current_processing_time_ms is None
    assert TimerContext(handle).current_watermark_ms is None


def test_typed_schema_rows_expose_values_and_validate_nullability() -> None:
    class Row:
        def asDict(self, recursive: bool = False) -> dict[str, object]:
            return {"customer_id": "customer-1"}

    event = cast(Input, _schema_instance(Input, Row()))
    assert event.customer_id == "customer-1"
    assert _schema_values(State, State(total="12")) == ("12",)

    with pytest.raises(ValueError, match="non-nullable.*None"):
        _schema_instance(Input, {"customer_id": None})
    with pytest.raises(ValueError, match="non-nullable.*None"):
        _schema_values(State, State(total=cast(str, None)))


def test_typed_callbacks_are_validated_before_operator_construction() -> None:
    class Valid:
        def on_rows(self, key, rows, state, timers):
            return iter(())

        def on_timer(self, key, timer, state, timers):
            return iter(())

    class Missing:
        pass

    class WrongSignature:
        def on_rows(self, key, rows, state):
            return iter(())

    class WrongTimerSignature:
        def on_rows(self, key, rows, state, timers):
            return iter(())

        def on_timer(self, key, timer, state):
            return iter(())

    _validate_typed_callbacks(Valid, "row")
    with pytest.raises(TypeError, match="on_rows.*required.*callable"):
        _validate_typed_callbacks(Missing, "row")
    with pytest.raises(TypeError, match=r"on_rows.*\(self, key, rows, state, timers\)"):
        _validate_typed_callbacks(WrongSignature, "row")
    with pytest.raises(TypeError, match=r"on_timer.*\(self, key, timer, state, timers\)"):
        _validate_typed_callbacks(WrongTimerSignature, "row")


def test_typed_row_outputs_convert_zero_and_multiple_rows_and_check_schema_and_nulls() -> None:
    def as_dict(**values):
        return values

    output_rows = _output_rows(
        iter((Output(total="3"), Output(total="7"))), output_schema=Output, row_type=as_dict
    )
    assert list(output_rows) == [{"total": "3"}, {"total": "7"}]
    assert list(_output_rows(iter(()), output_schema=Output, row_type=as_dict)) == []

    with pytest.raises(TypeError, match="must yield Output values"):
        list(_output_rows(iter((Input(customer_id="wrong"),)), output_schema=Output, row_type=as_dict))
    with pytest.raises(ValueError, match="non-nullable.*None"):
        list(_output_rows(iter((Output(total=cast(str, None)),)), output_schema=Output, row_type=as_dict))


@pytest.mark.parametrize(
    "profile, supported",
    [(">=4.0,<4.1", False), (">=4.1,<4.2", True)],
)
def test_row_transform_with_state_capability_matches_pinned_python_api(profile: str, supported: bool) -> None:
    capabilities = PySparkCapabilities(target_profile=profile)

    decision = capabilities.supports(CapabilityRequirement(group="streaming", name="transform_with_state"))
    assert decision.supported is supported


def test_spark_free_state_api_import_does_not_load_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    from structure.plugin.pyspark import transform_with_state as public_transform_with_state

    assert callable(public_transform_with_state)
    assert {name for name in sys.modules if name.startswith("pyspark")} == before


def test_row_state_supports_composite_grouping_keys() -> None:
    class CompositeTotals(Transform):
        events = input(CompositeInput, streaming=True)
        totals = output(CompositeOutput)

        @step(input=events, output=totals)
        def accumulate(self, event: CompositeInput) -> CompositeOutput:
            return transform_with_state(
                key=(event.customer_id, event.region),
                processor=CompositeCounter,
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    compiled = Compiler.frontend.compile()(
        CompositeTotals,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
    )
    state = cast(PySparkExecutionPlan, compiled.lowered).steps[0].operations[0].stateful_transform
    assert state is not None and isinstance(state.key, tuple) and len(state.key) == 2
