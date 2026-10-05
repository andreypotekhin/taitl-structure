from __future__ import annotations

import sys
from pathlib import Path
from typing import cast

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
    StateProcessor,
    external_state_processor,
    state_processor,
    transform_with_state,
)


class Input(Schema):
    customer_id = string(nullable=False)


class Key(Schema):
    customer_id = string(nullable=False)


class State(Schema):
    total = string(nullable=False)


class Output(Schema):
    total = string(nullable=False)


@state_processor
class Counter(StateProcessor[Input, Key, State, Output]):
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
    assert Counter.__structure_state_processor__ == (Input, Key, State, Output)


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


def test_typed_processor_requires_explicit_schema_type_parameters() -> None:
    with pytest.raises(TypeError, match=r"StateProcessor\[InputSchema, KeySchema"):

        @state_processor
        class MissingSchemas(StateProcessor):
            pass


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
