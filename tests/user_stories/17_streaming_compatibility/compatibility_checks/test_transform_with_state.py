from __future__ import annotations

from typing import Iterator, cast

from structure import Schema, Transform, input, output, step
from structure.core.compiler.api import Compiler
from structure.core.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.plugin.pyspark import StateProcessor, ValueState, integer, state_processor, string, transform_with_state
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


@state_processor
class AccountTotals(StateProcessor[Event, AccountKey, Total]):
    total: ValueState[TotalState]

    def on_rows(
        self,
        key: AccountKey,
        rows: Iterator[Event],
        timers,
    ) -> Iterator[Total]:
        raise NotImplementedError("Processor bodies run on Spark workers.")


class AccountTotalTransform(Transform):
    events = input(Event, streaming=True)
    totals = output(Total)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> Total:
        return transform_with_state(
            key=event.account_id,
            processor=AccountTotals,
            output_mode="Append",
            time_mode="None",
        )


def test_i_can_compile_typed_row_state_on_the_admitted_pyspark_profile() -> None:
    compiled = Compiler.frontend.compile()(
        AccountTotalTransform,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
    )
    plan = cast(PySparkExecutionPlan, compiled.lowered)
    operation = plan.steps[0].operations[0].stateful_transform

    assert operation is not None
    assert operation.output_mode == "Append"
    assert operation.time_mode == "None"
    report = Compiler.compileability.streaming()(plan, required=True)
    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()
