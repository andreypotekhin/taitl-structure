from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

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
from structure.plugin.pyspark.dsl.field import string
from structure.plugin.pyspark.dsl.Stateful import (
    ExternalStateProcessor,
    PandasStateProcessor,
    StateProcessor,
    external_state_processor,
    pandas_state_processor,
    state_processor,
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


@pandas_state_processor
class PandasCounter(PandasStateProcessor[Input, Key, State, Output]):
    pass


@state_processor
class RowCounter(StateProcessor[Input, Key, State, Output]):
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
    assert PandasCounter.__structure_pandas_state_processor__ == (Input, Key, State, Output)


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

    with pytest.raises(TypeError, match="@pandas_state_processor"):
        Compiler.frontend.compile()(
            WrongProcessor,
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
