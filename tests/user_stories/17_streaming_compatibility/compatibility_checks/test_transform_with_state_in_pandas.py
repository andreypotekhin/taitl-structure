from __future__ import annotations

from typing import Iterator, cast

import pytest

from structure import Schema, Transform, input, output, step
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    PandasStateProcessor,
    TimerContext,
    ValueState,
    integer,
    pandas_state_processor,
    string,
    transform_with_state_in_pandas,
)
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


@pandas_state_processor
class AccountTotals(PandasStateProcessor[Event, AccountKey, TotalState, TotalOutput]):
    def on_batches(self, key, batches, state: ValueState[TotalState], timers: TimerContext) -> Iterator[object]:
        raise NotImplementedError("Processor callbacks run on Spark workers.")


class AccountTotalTransform(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state_in_pandas(
            key=event.account_id,
            processor=AccountTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


@pytest.mark.parametrize("profile", (">=4.0,<4.1", ">=4.1,<4.2"))
def test_i_can_compile_typed_pandas_state_on_supported_ordinary_profiles(profile: str) -> None:
    compiled = Compiler.frontend.compile()(
        AccountTotalTransform,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": profile, "variant": "ordinary"}},
    )
    plan = cast(PySparkExecutionPlan, compiled.lowered)
    operation = plan.steps[0].operations[0].stateful_transform

    assert operation is not None
    assert operation.interface == "pandas"
    assert operation.output_mode == "Update"
    assert operation.time_mode == "ProcessingTime"
