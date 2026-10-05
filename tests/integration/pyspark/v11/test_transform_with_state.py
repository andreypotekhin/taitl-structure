from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
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
    Timer,
    TimerContext,
    ValueState,
    external_state_processor,
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


class EventCount(Schema):
    count = integer(nullable=False)


class InitialTotal(Schema):
    customer_id = string(nullable=False)
    total = integer(nullable=False)
    count = integer(nullable=False)


class NativeTotalOutput(Schema):
    customer_id = string(nullable=False)
    total = integer(nullable=False)
    count = integer(nullable=False)
    reason = string(nullable=False)


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


@state_processor
class TimerTotals(StateProcessor[Event, CustomerKey, CustomerTotal, TotalOutput]):
    def on_rows(
        self,
        key: CustomerKey,
        rows: Iterator[Event],
        state: ValueState[CustomerTotal],
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        total = 0
        for row in rows:
            total += row.amount
        state.update(CustomerTotal(total=total))
        current_time = timers.current_processing_time_ms
        assert current_time is not None
        timers.register(current_time + 100)
        yield TotalOutput(customer_id=key.customer_id, total=total)

    def on_timer(
        self,
        key: CustomerKey,
        timer: Timer,
        state: ValueState[CustomerTotal],
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        current = state.get()
        if current is not None:
            yield TotalOutput(customer_id=key.customer_id, total=current.total)

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


class EmitTimerTotal(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state(
            key=event.customer_id,
            processor=TimerTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


class AccumulateNativeTotals(Transform):
    events = input(Event, streaming=True)
    initial = input(InitialTotal)
    totals = output(NativeTotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> NativeTotalOutput:
        from integration.pyspark.v11.native_state_processors import NativeInitialTotals

        processor = external_state_processor(
            NativeInitialTotals,
            input=Event,
            key=CustomerKey,
            states=(CustomerTotal, EventCount),
            output=NativeTotalOutput,
        )
        return transform_with_state(
            key=event.customer_id,
            processor=processor,
            output_mode="Update",
            time_mode="ProcessingTime",
            initial_state=self.initial,
        )


def test_row_state_runs_online_and_generated_and_resumes_checkpoint(spark, tmp_path, integration_shared_dir) -> None:
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

    with TemporaryDirectory(prefix=f"row-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
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
                    ).trigger(once=True).start()
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

                assert sorted(emitted) == [("c-1", 3), ("c-1", 7)]


def test_typed_state_timer_emits_after_registration_online_and_generated(spark, tmp_path, integration_shared_dir) -> None:
    files = render_generated_projects(
        ((EmitTimerTotal, f"{SOURCE_MODULE}.EmitTimerTotal"),),
        generated_package="integration_v11_timed_state_generated",
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )

    with TemporaryDirectory(prefix=f"typed-timer-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, "integration_v11_timed_state_generated", files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                (source / "event.json").write_text(
                    json.dumps({"customer_id": "c-1", "amount": 3}) + "\n",
                    encoding="utf-8",
                )
                events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                result = EmitTimerTotal(events=events).run(
                    session(spark, execution_mode=mode, generated_package="integration_v11_timed_state_generated")
                )
                emitted: list[tuple[str, int]] = []

                def collect_batch(frame, _batch_id: int) -> None:
                    emitted.extend((row.customer_id, row.total) for row in frame.collect())

                query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                    "checkpointLocation", str(root / mode / "checkpoint")
                ).trigger(processingTime="100 milliseconds").start()
                deadline = monotonic() + 30
                try:
                    while len(emitted) < 2 and monotonic() < deadline and query.isActive:
                        sleep(0.1)
                    assert len(emitted) >= 2, "Typed processor timer did not emit before the deadline"
                finally:
                    query.stop()

                assert sorted(emitted) == [("c-1", 3), ("c-1", 3)]


def test_native_state_uses_multiple_states_and_initial_state_online_and_generated(
    spark, tmp_path, integration_shared_dir
) -> None:
    files = render_generated_projects(
        ((AccumulateNativeTotals, f"{SOURCE_MODULE}.AccumulateNativeTotals"),),
        generated_package="integration_v11_native_state_generated",
        source_schema_modules={SOURCE_MODULE: [Event, InitialTotal, NativeTotalOutput]},
    )
    generated_source = "\n".join(files.values())
    assert "NativeInitialTotals" in generated_source
    assert "initial_state=" in generated_source

    with TemporaryDirectory(prefix=f"native-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, "integration_v11_native_state_generated", files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                emitted: list[tuple[str, int, int, str]] = []

                (source / "events.json").write_text(
                    json.dumps({"customer_id": "c-1", "amount": 2})
                    + "\n"
                    + json.dumps({"customer_id": "c-1", "amount": 1})
                    + "\n",
                    encoding="utf-8",
                )
                events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                initial = spark.createDataFrame(
                    [("c-1", 5, 10)],
                    "customer_id STRING NOT NULL, total INT NOT NULL, count INT NOT NULL",
                )
                result = AccumulateNativeTotals(events=events, initial=initial).run(
                    session(
                        spark,
                        execution_mode=mode,
                        generated_package="integration_v11_native_state_generated",
                    )
                )

                def collect_batch(frame, _batch_id: int) -> None:
                    emitted.extend((row.customer_id, row.total, row["count"], row.reason) for row in frame.collect())

                query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                    "checkpointLocation", str(root / mode / "checkpoint")
                ).trigger(processingTime="100 milliseconds").start()
                deadline = monotonic() + 30
                try:
                    while not any(row[3] == "timer" for row in emitted) and monotonic() < deadline and query.isActive:
                        sleep(0.1)
                    assert any(row[3] == "timer" for row in emitted), "Native processor timer did not fire"
                finally:
                    query.stop()

                assert sorted(emitted) == [
                    ("c-1", 8, 12, "input"),
                    ("c-1", 8, 12, "timer"),
                ]
