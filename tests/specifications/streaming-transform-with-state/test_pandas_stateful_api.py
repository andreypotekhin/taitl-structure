from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Generic, TypeVar, cast

import pytest

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from structure import Schema, Transform, input, output, step
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import BackendCapabilityError, CapabilityRequirement
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.field import integer, string
from structure.plugin.pyspark.dsl.Stateful import (
    ExternalStateProcessor,
    PandasStateProcessor,
    StateProcessor,
    ValueState,
    external_state_processor,
    pandas_state_processor,
    transform_with_state_in_pandas,
)


class Input(Schema):
    customer_id = string(nullable=False)


class Key(Schema):
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


class SingleCompositeKey(Schema):
    customer_id = string(nullable=False)


class WrongTypeCompositeKey(Schema):
    customer_id = string(nullable=False)
    region = integer(nullable=False)


class CompositeOutput(Schema):
    total = string(nullable=False)


class CompositeCounter(PandasStateProcessor[CompositeInput, CompositeKey, State, CompositeOutput]):
    pass


class SingleKeyCounter(PandasStateProcessor[CompositeInput, SingleCompositeKey, State, CompositeOutput]):
    pass


class WrongTypeKeyCounter(PandasStateProcessor[CompositeInput, WrongTypeCompositeKey, State, CompositeOutput]):
    pass


InputSchema = TypeVar("InputSchema", bound=Schema)
KeySchema = TypeVar("KeySchema", bound=Schema)
StateSchema = TypeVar("StateSchema", bound=Schema)
OutputSchema = TypeVar("OutputSchema", bound=Schema)


class GenericPandasCounter(
    PandasStateProcessor[InputSchema, KeySchema, StateSchema, OutputSchema],
    Generic[InputSchema, KeySchema, StateSchema, OutputSchema],
):
    pass


class PandasCounter(GenericPandasCounter[Input, Key, State, Output]):
    pass


class UnresolvedPandasCounter(GenericPandasCounter):
    pass


@pandas_state_processor
class DecoratedPandasCounter(PandasStateProcessor[Input, Key, State, Output]):
    pass


class RowCounter(StateProcessor[Input, Key, Output]):
    total: ValueState[State]

    def on_rows(self, key, rows, timers):
        return iter(())
    pass


class StreamingPandasTotals(Transform):
    events = input(Input, streaming=True)
    output_schema = output(Output)

    @step(input=events, output=output_schema)
    def calculate(self, row: Input) -> Output:
        return transform_with_state_in_pandas(
            key=row.customer_id,
            processor=PandasCounter,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


def test_pandas_processor_schema_hints_are_the_source_of_truth() -> None:
    from structure.plugin.pyspark.dsl.Stateful import _processor_schemas

    assert _processor_schemas(PandasCounter, PandasStateProcessor) == (Input, Key, State, Output)
    assert DecoratedPandasCounter.__structure_pandas_state_processor__ == (Input, Key, State, Output)


@pytest.mark.parametrize(
    "frames, error_type, message",
    [
        ([object()], TypeError, "must yield pandas.DataFrame"),
        ([{"columns": ["wrong"], "empty": False}], ValueError, r"expected \['total'\], received \['wrong'\]"),
    ],
)
def test_pandas_state_validates_output_frames_without_importing_pandas(
    monkeypatch, frames, error_type: type[Exception], message: str
) -> None:
    import sys
    from types import ModuleType

    from structure.plugin.pyspark.execution.stateful import _output_pandas_frames

    class FakeDataFrame:
        def __init__(self, *, columns: list[str]):
            self.columns = columns
            self.empty = True

    pandas = ModuleType("pandas")
    pandas.DataFrame = FakeDataFrame  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pandas", pandas)
    values = [
        FakeDataFrame(columns=frame["columns"]) if isinstance(frame, dict) else frame
        for frame in frames
    ]

    with pytest.raises(error_type, match=message):
        list(_output_pandas_frames(values, Output, callback_name="on_batches"))


def test_pandas_state_normalizes_empty_output_frame_without_columns(monkeypatch) -> None:
    import sys
    from types import ModuleType

    from structure.plugin.pyspark.execution.stateful import _output_pandas_frames

    class FakeDataFrame:
        def __init__(self, *, columns: list[str]):
            self.columns = columns
            self.empty = not columns

    pandas = ModuleType("pandas")
    pandas.DataFrame = FakeDataFrame  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pandas", pandas)

    output_frames = list(_output_pandas_frames([FakeDataFrame(columns=[])], Output, callback_name="on_batches"))

    assert len(output_frames) == 1
    assert output_frames[0].columns == ["total"]


def test_pandas_processor_rejects_unresolved_inherited_type_variables() -> None:
    class UnresolvedPandasTotals(Transform):
        events = input(Input, streaming=True)
        output_schema = output(Output)

        @step(input=events, output=output_schema)
        def calculate(self, row: Input) -> Output:
            return transform_with_state_in_pandas(
                key=row.customer_id,
                processor=UnresolvedPandasCounter,
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    with pytest.raises(TypeError, match="concrete Structure Schema classes"):
        Compiler.frontend.compile()(
            UnresolvedPandasTotals,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
        )


def test_transform_with_state_in_pandas_lowers_and_renders_for_spark_4_0() -> None:
    compiled = Compiler.frontend.compile()(
        StreamingPandasTotals,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
    )
    plan = cast(PySparkExecutionPlan, compiled.lowered)
    state = plan.steps[0].operations[0].stateful_transform
    assert state is not None
    assert state.interface == "pandas"
    assert state.processor_mode == "typed"
    assert state.key_schema is Key
    assert state.state_schema is State
    assert state.output_schema is Output
    assert state.output_mode == "Update"

    generated_modules = PySpark.render.project()(
        plan,
        source_transform=f"{__name__}.StreamingPandasTotals",
        generated_package="pandas_stateful_generated",
        source_schema_modules={__name__: [Input, Output]},
    )
    generated = "\n".join(generated_modules.values())
    assert "apply_stateful_transform(" in generated
    assert "interface='pandas'" in generated
    assert f"{__name__}:PandasCounter" in generated
    for module in generated_modules.values():
        compile(module, "<generated-transform-with-state-in-pandas>", "exec")


def test_pandas_processor_lowers_for_spark_4_1() -> None:
    compiled = Compiler.frontend.compile()(
        StreamingPandasTotals,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
    )
    plan = cast(PySparkExecutionPlan, compiled.lowered)
    state = plan.steps[0].operations[0].stateful_transform
    assert state is not None and state.interface == "pandas"


def test_pandas_state_operation_is_rejected_for_spark_3_5_and_connect() -> None:
    for profile, variant in ((">=3.5,<4.1", "ordinary"), (">=4.0,<4.1", "spark-connect")):
        with pytest.raises(BackendCapabilityError):
            Compiler.frontend.compile()(
                StreamingPandasTotals,
                materialize_schemas=False,
                plugin={"pyspark": {"profile": profile, "variant": variant}},
            )


def test_row_processor_cannot_be_passed_to_pandas_operation() -> None:
    class WrongProcessor(Transform):
        events = input(Input, streaming=True)
        output_schema = output(Output)

        @step(input=events, output=output_schema)
        def calculate(self, row: Input) -> Output:
            return transform_with_state_in_pandas(
                key=row.customer_id,
                processor=cast(Any, RowCounter),
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    with pytest.raises(TypeError, match="received StateProcessor"):
        Compiler.frontend.compile()(
            WrongProcessor,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
        )


def test_pandas_processor_cannot_be_passed_to_row_operation() -> None:
    class WrongProcessor(Transform):
        events = input(Input, streaming=True)
        output_schema = output(Output)

        @step(input=events, output=output_schema)
        def calculate(self, row: Input) -> Output:
            from structure.plugin.pyspark.dsl.Stateful import transform_with_state

            return transform_with_state(
                key=row.customer_id,
                processor=cast(Any, PandasCounter),
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    with pytest.raises(TypeError, match="received PandasStateProcessor"):
        Compiler.frontend.compile()(
            WrongProcessor,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
        )


def test_pandas_state_supports_composite_grouping_keys() -> None:
    class CompositeTotals(Transform):
        events = input(CompositeInput, streaming=True)
        output_schema = output(CompositeOutput)

        @step(input=events, output=output_schema)
        def calculate(self, row: CompositeInput) -> CompositeOutput:
            return transform_with_state_in_pandas(
                key=(row.customer_id, row.region),
                processor=CompositeCounter,
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    compiled = Compiler.frontend.compile()(
        CompositeTotals,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
    )
    plan = cast(PySparkExecutionPlan, compiled.lowered)
    state = plan.steps[0].operations[0].stateful_transform
    assert state is not None and isinstance(state.key, tuple) and len(state.key) == 2

    generated_modules = PySpark.render.project()(
        plan,
        source_transform=f"{__name__}.CompositeTotals",
        generated_package="composite_pandas_state_generated",
        source_schema_modules={__name__: [CompositeInput, CompositeOutput]},
    )
    generated = "\n".join(generated_modules.values())
    assert "key=(" in generated
    assert "customer_id" in generated and "region" in generated
    for module in generated_modules.values():
        compile(module, "<generated-composite-pandas-state>", "exec")


@pytest.mark.parametrize("key_factory", ["empty", "duplicate", "expression"])
def test_pandas_state_rejects_invalid_composite_keys(key_factory: str) -> None:
    class InvalidCompositeKey(Transform):
        events = input(CompositeInput, streaming=True)
        output_schema = output(CompositeOutput)

        @step(input=events, output=output_schema)
        def calculate(self, row: CompositeInput) -> CompositeOutput:
            keys = {
                "empty": (),
                "duplicate": (row.customer_id, row.customer_id),
                "expression": (row.customer_id, row.customer_id + "suffix"),
            }[key_factory]
            return transform_with_state_in_pandas(
                key=keys,
                processor=CompositeCounter,
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    expected = "non-empty tuple" if key_factory == "empty" else "duplicate" if key_factory == "duplicate" else "only fields"
    with pytest.raises(TypeError, match=expected):
        Compiler.frontend.compile()(
            InvalidCompositeKey,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
        )


@pytest.mark.parametrize("processor", [SingleKeyCounter, WrongTypeKeyCounter])
def test_pandas_state_rejects_composite_key_schema_mismatch(processor) -> None:
    class MismatchedKeySchema(Transform):
        events = input(CompositeInput, streaming=True)
        output_schema = output(CompositeOutput)

        @step(input=events, output=output_schema)
        def calculate(self, row: CompositeInput) -> CompositeOutput:
            return transform_with_state_in_pandas(
                key=(row.customer_id, row.region),
                processor=processor,
                output_mode="Update",
                time_mode="ProcessingTime",
            )

    with pytest.raises(TypeError, match="key Schema fields must match.*count and type"):
        Compiler.frontend.compile()(
            MismatchedKeySchema,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
        )


def test_external_processor_binding_is_shared_with_pandas_operation() -> None:
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


@pytest.mark.parametrize(
    "profile, variant, supported",
    [
        (">=4.0,<4.1", "ordinary", True),
        (">=4.1,<4.2", "ordinary", True),
        (">=4.0,<4.1", "spark-connect", False),
        (">=3.5,<4.1", "ordinary", False),
    ],
)
def test_pandas_state_capability_matches_supported_profiles(profile: str, variant: str, supported: bool) -> None:
    capabilities = PySparkCapabilities(target_profile=profile, target_variant=variant)
    decision = capabilities.supports(
        CapabilityRequirement(group="streaming", name="transform_with_state_in_pandas")
    )
    assert decision.supported is supported


def test_public_pandas_state_api_import_does_not_load_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}
    from structure.plugin.pyspark import PandasStateProcessor as PublicPandasStateProcessor
    from structure.plugin.pyspark import pandas_state_processor as public_pandas_state_processor
    from structure.plugin.pyspark import transform_with_state_in_pandas as public_transform_with_state_in_pandas

    assert PublicPandasStateProcessor is PandasStateProcessor
    assert callable(public_pandas_state_processor)
    assert callable(public_transform_with_state_in_pandas)
    assert {name for name in sys.modules if name.startswith("pyspark")} == before
