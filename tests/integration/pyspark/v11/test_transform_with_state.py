from __future__ import annotations

import json
from collections.abc import Iterator
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from structure import Schema, Transform, input, output, step
from structure.plugin.pyspark import (
    StateProcessor,
    TimerContext,
    ValueState,
    integer,
    state_processor,
    string,
    transform_with_state,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(backend_name() != "pyspark41", reason="Row transformWithState requires ordinary PySpark 4.1"),
]

SOURCE_MODULE = "integration.pyspark.v11.test_transform_with_state"
GENERATED_PACKAGE = "integration_v11_row_state_generated"


class Event(Schema):
    customer_id = string(nullable=False)
    amount = integer(nullable=False)


class CustomerKey(Schema):
    customer_id = string(nullable=False)


class CustomerTotal(Schema):
    total = integer(nullable=False)


class TotalOutput(Schema):
    customer_id = string(nullable=False)
    total = integer(nullable=False)


@state_processor
class CustomerTotals(StateProcessor[Event, CustomerKey, CustomerTotal, TotalOutput]):
    def on_rows(
        self,
        key: CustomerKey,
        rows: Iterator[Event],
        state: ValueState[CustomerTotal],
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        current = state.get()
        total = 0 if current is None else current.total
        for row in rows:
            total += row.amount
        state.update(CustomerTotal(total=total))
        yield TotalOutput(customer_id=key.customer_id, total=total)


class AccumulateCustomerTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state(
            key=event.customer_id,
            processor=CustomerTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


def test_row_state_runs_online_and_generated_and_resumes_checkpoint(spark, tmp_path) -> None:
    files = render_generated_projects(
        ((AccumulateCustomerTotals, f"{SOURCE_MODULE}.AccumulateCustomerTotals"),),
        generated_package=GENERATED_PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )
    generated_source = "\n".join(files.values())
    assert "interface='row'" in generated_source
    assert "apply_stateful_transform(" in generated_source
    for token in ("readStream", "writeStream", "checkpointLocation", "toPandas(", ".rdd"):
        assert token not in generated_source

    root = tmp_path / f"row-state-{uuid4().hex}"
    with generated_project(tmp_path, GENERATED_PACKAGE, files):
        for mode in ("online", "generated"):
            source = root / mode / "source"
            source.mkdir(parents=True)
            checkpoint = root / mode / "checkpoint"
            emitted: list[tuple[str, int]] = []

            def run_available_now() -> None:
                events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                result = AccumulateCustomerTotals(events=events).run(
                    session(spark, execution_mode=mode, generated_package=GENERATED_PACKAGE)
                )

                def collect_batch(frame, _batch_id: int) -> None:
                    emitted.extend((row.customer_id, row.total) for row in frame.collect())

                query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                    "checkpointLocation", str(checkpoint)
                ).trigger(availableNow=True).start()
                try:
                    assert query.awaitTermination(120), "Row state query did not complete"
                finally:
                    query.stop()

            (source / "first.json").write_text(
                json.dumps({"customer_id": "c-1", "amount": 2})
                + "\n"
                + json.dumps({"customer_id": "c-1", "amount": 1})
                + "\n",
                encoding="utf-8",
            )
            run_available_now()
            (source / "second.json").write_text(
                json.dumps({"customer_id": "c-1", "amount": 4}) + "\n",
                encoding="utf-8",
            )
            run_available_now()

            assert emitted == [("c-1", 3), ("c-1", 7)]
