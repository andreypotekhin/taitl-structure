from __future__ import annotations

from typing import Iterator

from structure import Schema, Transform, input, output, step
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    PandasGroupState,
    PandasGroupStateProcessor,
    apply_in_pandas_with_state,
    integer,
    string,
)
from structure.plugin.pyspark.compiler.commands.ClassifyStreamingCompatibility import ClassifyStreamingCompatibility
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Event(Schema):
    account_id = string(nullable=False)
    amount = integer(nullable=False)


class AccountKey(Schema):
    account_id = string(nullable=False)


class TotalState(Schema):
    total = integer(nullable=False)


class Total(Schema):
    account_id = string(nullable=False)
    total = integer(nullable=False)


class AccountTotals(PandasGroupStateProcessor[Event, AccountKey, TotalState, Total]):
    def on_batches(
        self,
        key: AccountKey,
        batches,
        state: PandasGroupState[TotalState],
    ) -> Iterator[object]:
        raise NotImplementedError("Processor bodies run on Spark workers.")


class StatefulTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(Total)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> Total:
        return apply_in_pandas_with_state(
            key=event.account_id,
            processor=AccountTotals,
            output_mode="Update",
            timeout="none",
        )


def test_i_can_declare_typed_legacy_pandas_state_in_a_streaming_transform() -> None:
    compiled = Compiler.frontend.compile()(
        StatefulTotals,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=3.5,<4.0", "variant": "ordinary"}},
    )
    plan = compiled.lowered
    assert isinstance(plan, PySparkExecutionPlan)
    assert plan.steps[0].operations[0].legacy_pandas_state is not None

    report = ClassifyStreamingCompatibility()(plan, required=True)
    assert not report.findings
