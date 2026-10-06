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
from structure.core.compiler.diagnostics.model.StructureCompileError import StructureCompileError
from structure.plugin.api.v1.model import BackendCapabilityError, CapabilityRequirement
from structure.plugin.pyspark import (
    PandasGroupState,
    PandasGroupStateProcessor,
    PySpark,
    apply_in_pandas_with_state,
    external_pandas_state_function,
    integer,
    string,
)
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Event(Schema):
    account_id = string(nullable=False)
    amount = integer(nullable=False)


class AccountKey(Schema):
    account_id = string(nullable=False)


class TotalState(Schema):
    total = integer(nullable=False)


class TotalOutput(Schema):
    account_id = string(nullable=False)
    total = integer(nullable=False)


InputSchema = TypeVar("InputSchema", bound=Schema)
KeySchema = TypeVar("KeySchema", bound=Schema)
StateSchema = TypeVar("StateSchema", bound=Schema)
OutputSchema = TypeVar("OutputSchema", bound=Schema)


class GenericPandasCounter(
    PandasGroupStateProcessor[InputSchema, KeySchema, StateSchema, OutputSchema],
    Generic[InputSchema, KeySchema, StateSchema, OutputSchema],
):
    pass


class PandasCounter(GenericPandasCounter[Event, AccountKey, TotalState, TotalOutput]):
    def on_batches(self, key, batches, state: PandasGroupState[TotalState]):
        import pandas as pd  # type: ignore[import-untyped]

        current = state.get()
        total = 0 if current is None else current.total
        for batch in batches:
            total += int(batch["amount"].sum())
        state.update(TotalState(total=total))
        yield pd.DataFrame({"account_id": [key.account_id], "total": [total]})


def native_pandas_counter(key, batches, state):
    import pandas as pd  # type: ignore[import-untyped]

    total = state.get[0] if state.exists else 0
    for batch in batches:
        total += int(batch["amount"].sum())
    state.update((total,))
    yield pd.DataFrame({"account_id": [key[0]], "total": [total]})


class TypedTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return apply_in_pandas_with_state(
            key=event.account_id,
            processor=PandasCounter,
            output_mode="Update",
            timeout="none",
        )


class NativeTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return apply_in_pandas_with_state(
            key=event.account_id,
            processor=external_pandas_state_function(
                native_pandas_counter,
                input=Event,
                key=AccountKey,
                state=TotalState,
                output=TotalOutput,
            ),
            output_mode="Update",
            timeout="none",
        )


def _compile(transform: type[Transform], *, profile: str = ">=3.5,<4.1", variant: str = "ordinary"):
    return Compiler.frontend.compile()(
        transform,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": profile, "variant": variant}},
    )


def test_typed_legacy_processor_resolves_schemas_and_renders_public_call() -> None:
    compilation = _compile(TypedTotals)
    plan = cast(PySparkExecutionPlan, compilation.lowered)
    state = plan.steps[0].operations[0].legacy_pandas_state

    assert state is not None
    assert state.processor_mode == "typed"
    assert state.key_schema is AccountKey
    assert state.state_schema is TotalState
    assert state.output_schema is TotalOutput
    assert state.timeout == "none"

    generated = "\n".join(
        PySpark.render.project()(
            plan,
            source_transform=f"{__name__}.TypedTotals",
            generated_package="legacy_pandas_state_generated",
            source_schema_modules={__name__: [Event, TotalOutput]},
        ).values()
    )
    assert "apply_legacy_pandas_state(" in generated
    assert "applyInPandasWithState" not in generated  # dispatch stays in the shared runtime adapter
    assert f"{__name__}:PandasCounter" in generated


def test_native_legacy_processor_binding_lowers_schema_and_timeout_metadata() -> None:
    compilation = _compile(NativeTotals, profile=">=4.0,<4.1")
    plan = cast(PySparkExecutionPlan, compilation.lowered)
    state = plan.steps[0].operations[0].legacy_pandas_state

    assert state is not None
    assert state.processor_mode == "native"
    assert state.processor.function is native_pandas_counter
    assert state.state_schema is TotalState
    assert state.timeout == "none"


@pytest.mark.parametrize("profile", [">=3.5,<4.0", ">=3.5,<4.1", ">=4.0,<4.1", ">=4.1,<4.2"])
def test_legacy_pandas_state_capability_is_admitted_on_ordinary_profiles(profile: str) -> None:
    capabilities = PySparkCapabilities(target_profile=profile, target_variant="ordinary")
    decision = capabilities.supports(
        CapabilityRequirement(group="streaming", name="apply_in_pandas_with_state")
    )
    assert decision.supported


@pytest.mark.parametrize("profile", [">=3.5,<4.0", ">=4.0,<4.1", ">=4.1,<4.2"])
def test_legacy_pandas_state_is_rejected_on_connect(profile: str) -> None:
    with pytest.raises(BackendCapabilityError):
        _compile(TypedTotals, profile=profile, variant="spark-connect")


@pytest.mark.parametrize("output_mode", ["Complete", "complete", "Append "])
def test_legacy_pandas_state_rejects_unsupported_output_modes(output_mode: str) -> None:
    class InvalidMode(Transform):
        events = input(Event, streaming=True)
        totals = output(TotalOutput)

        @step(input=events, output=totals)
        def accumulate(self, event: Event) -> TotalOutput:
            return apply_in_pandas_with_state(
                key=event.account_id,
                processor=PandasCounter,
                output_mode=output_mode,
                timeout="none",
            )

    with pytest.raises(StructureCompileError, match="Append.*Update"):
        _compile(InvalidMode)


class FakeGroupState:
    exists = True
    hasTimedOut = False

    def __init__(self) -> None:
        self.value = (4,)
        self.timeout_duration: int | None = None
        self.timeout_timestamp: int | None = None
        self.removed = False

    @property
    def getOption(self):
        return self.value if not self.removed else None

    def update(self, value) -> None:
        self.value = tuple(value)
        self.removed = False

    def remove(self) -> None:
        self.removed = True

    def setTimeoutDuration(self, duration_ms: int) -> None:
        self.timeout_duration = duration_ms

    def setTimeoutTimestamp(self, timestamp_ms: int) -> None:
        self.timeout_timestamp = timestamp_ms

    def getCurrentProcessingTimeMs(self) -> int:
        return 100

    def getCurrentWatermarkMs(self) -> int:
        return 200


def test_typed_group_state_converts_schema_and_guards_timeout_modes() -> None:
    handle = FakeGroupState()
    state = PandasGroupState[TotalState](handle, TotalState, "processing_time")

    assert state.exists
    assert state.get() is not None and state.get().total == 4
    state.update(TotalState(total=7))
    state.set_timeout_duration(5_000)
    state.remove()

    assert handle.value == (7,)
    assert handle.timeout_duration == 5_000
    assert handle.removed
    assert state.current_processing_time_ms == 100
    with pytest.raises(ValueError, match="timeout='event_time'"):
        state.set_timeout_timestamp(10_000)
