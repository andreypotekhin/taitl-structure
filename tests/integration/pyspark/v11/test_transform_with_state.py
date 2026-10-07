from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import timedelta
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
    ListState,
    MapState,
    StateProcessor,
    Timer,
    TimerContext,
    ValueState,
    external_state_processor,
    integer,
    map_state,
    state_processor,
    string,
    timestamp,
    transform_with_state,
    value_state,
    watermark,
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


class AmountKey(Schema):
    amount = integer(nullable=False)


class AmountCount(Schema):
    count = integer(nullable=False)


class TotalOutput(Schema):
    customer_id = string(nullable=False)
    total = integer(nullable=False)


class EventCount(Schema):
    count = integer(nullable=False)


class InitialTotal(Schema):
    customer_id = string(nullable=False)
    total = integer(nullable=False)
    count = integer(nullable=False)


class InitialSeed(Schema):
    customer_id = string(nullable=False)
    total = integer(nullable=False)


class NativeTotalOutput(Schema):
    customer_id = string(nullable=False)
    total = integer(nullable=False)
    count = integer(nullable=False)
    reason = string(nullable=False)


class CompositeEvent(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)
    amount = integer(nullable=False)


class CompositeKey(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)


class CompositeOutput(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)
    total = integer(nullable=False)


class EventTimeEvent(Schema):
    customer_id = string(nullable=False)
    event_time = timestamp(nullable=False)
    amount = integer(nullable=False)


@state_processor
class CustomerTotals(StateProcessor[Event, CustomerKey, TotalOutput]):
    total: ValueState[CustomerTotal]

    def on_rows(
        self,
        key: CustomerKey,
        rows: Iterator[Event],
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        current = self.total.get()
        total = 0 if current is None else current.total
        for row in rows:
            total += row.amount
        self.total.update(CustomerTotal(total=total))
        yield TotalOutput(customer_id=key.customer_id, total=total)


@state_processor
class TimerTotals(StateProcessor[Event, CustomerKey, TotalOutput]):
    total: ValueState[CustomerTotal]

    def on_rows(
        self,
        key: CustomerKey,
        rows: Iterator[Event],
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        total = 0
        for row in rows:
            total += row.amount
        self.total.update(CustomerTotal(total=total))
        current_time = timers.current_processing_time_ms
        assert current_time is not None
        timers.register(current_time + 100)
        yield TotalOutput(customer_id=key.customer_id, total=total)

    def on_timer(
        self,
        key: CustomerKey,
        timer: Timer,
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        current = self.total.get()
        if current is not None:
            yield TotalOutput(customer_id=key.customer_id, total=current.total)


@state_processor
class CompositeTotals(StateProcessor[CompositeEvent, CompositeKey, CompositeOutput]):
    total: ValueState[CustomerTotal]

    def on_rows(
        self,
        key: CompositeKey,
        rows: Iterator[CompositeEvent],
        timers: TimerContext,
    ) -> Iterator[CompositeOutput]:
        previous = self.total.get()
        total = 0 if previous is None else previous.total
        for row in rows:
            total += row.amount
        self.total.update(CustomerTotal(total=total))
        yield CompositeOutput(customer_id=key.customer_id, region=key.region, total=total)


@state_processor
class EventTimeTotals(StateProcessor[EventTimeEvent, CustomerKey, TotalOutput]):
    total: ValueState[CustomerTotal]

    def on_rows(
        self,
        key: CustomerKey,
        rows: Iterator[EventTimeEvent],
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        current = self.total.get()
        total = 0 if current is None else current.total
        for row in rows:
            total += row.amount
            timers.register(int(row.event_time.timestamp() * 1_000) + 5_000)
        self.total.update(CustomerTotal(total=total))
        return iter(())

    def on_timer(
        self,
        key: CustomerKey,
        timer: Timer,
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        current = self.total.get()
        if current is not None:
            yield TotalOutput(customer_id=key.customer_id, total=current.total)


@state_processor
class CollectionTotals(StateProcessor[Event, CustomerKey, TotalOutput]):
    total: ValueState[CustomerTotal] = value_state(ttl=timedelta(hours=1))
    recent: ListState[Event]
    by_amount: MapState[AmountKey, AmountCount]

    def on_rows(self, key: CustomerKey, rows: Iterator[Event], timers: TimerContext) -> Iterator[TotalOutput]:
        current = self.total.get()
        total = 0 if current is None else current.total
        for row in rows:
            total += row.amount
            self.recent.append_value(row)
            amount_key = AmountKey(amount=row.amount)
            previous = self.by_amount.get_value(amount_key)
            self.by_amount.update_value(
                amount_key,
                AmountCount(count=1 if previous is None else previous.count + 1),
            )
        self.total.update(CustomerTotal(total=total))
        # Read both collection handles as part of the end-to-end adapter check.
        assert sum(1 for _ in self.recent.get()) > 0
        assert sum(value.count for value in self.by_amount.values()) > 0
        yield TotalOutput(customer_id=key.customer_id, total=total)


@state_processor
class ExpiringTotals(StateProcessor[Event, CustomerKey, TotalOutput]):
    total: ValueState[CustomerTotal] = value_state(ttl=timedelta(seconds=2))

    def on_rows(self, key: CustomerKey, rows: Iterator[Event], timers: TimerContext) -> Iterator[TotalOutput]:
        current = self.total.get()
        total = 0 if current is None else current.total
        for row in rows:
            total += row.amount
        self.total.update(CustomerTotal(total=total))
        yield TotalOutput(customer_id=key.customer_id, total=total)


@state_processor
class TypedInitialTotals(StateProcessor[Event, CustomerKey, TotalOutput]):
    total: ValueState[CustomerTotal]

    def on_initial_state(self, key: CustomerKey, initial: InitialSeed, timers: TimerContext) -> None:
        self.total.update(CustomerTotal(total=initial.total))

    def on_rows(self, key: CustomerKey, rows: Iterator[Event], timers: TimerContext) -> Iterator[TotalOutput]:
        current = self.total.get()
        total = 0 if current is None else current.total
        for row in rows:
            total += row.amount
        self.total.update(CustomerTotal(total=total))
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


class AccumulateCollectionTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state(
            key=event.customer_id,
            processor=CollectionTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


class AccumulateExpiringTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state(
            key=event.customer_id,
            processor=ExpiringTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


class AccumulateTypedInitialTotals(Transform):
    events = input(Event, streaming=True)
    initial = input(InitialSeed)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state(
            key=event.customer_id,
            processor=TypedInitialTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
            initial_state=self.initial,
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


class AccumulateAppendTotals(Transform):
    events = input(CompositeEvent, streaming=True)
    totals = output(CompositeOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: CompositeEvent) -> CompositeOutput:
        return transform_with_state(
            key=(event.customer_id, event.region),
            processor=CompositeTotals,
            output_mode="Append",
            time_mode="None",
        )


class WatermarkEventTime(Transform):
    events = input(EventTimeEvent, streaming=True)
    watermarked = output(EventTimeEvent)

    @step(input=events, output=watermarked)
    def apply_watermark(self, event: EventTimeEvent) -> EventTimeEvent:
        watermark(event.event_time, delay="1 seconds")
        return event


class EmitEventTimeTotals(Transform):
    events = input(EventTimeEvent, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: EventTimeEvent) -> TotalOutput:
        return transform_with_state(
            key=event.customer_id,
            processor=EventTimeTotals,
            output_mode="Update",
            time_mode="EventTime",
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


def test_typed_value_list_map_state_run_generated_and_restart(spark, tmp_path, integration_shared_dir) -> None:
    generated_package = "integration_v11_collection_state_generated"
    files = render_generated_projects(
        ((AccumulateCollectionTotals, f"{SOURCE_MODULE}.AccumulateCollectionTotals"),),
        generated_package=generated_package,
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )

    with TemporaryDirectory(prefix=f"collection-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, generated_package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                checkpoint = root / mode / "checkpoint"
                emitted: list[tuple[str, int]] = []

                def run_available_now() -> None:
                    events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                    result = AccumulateCollectionTotals(events=events).run(
                        session(spark, execution_mode=mode, generated_package=generated_package)
                    )

                    def collect_batch(frame, _batch_id: int) -> None:
                        emitted.extend((row.customer_id, row.total) for row in frame.collect())

                    query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                        "checkpointLocation", str(checkpoint)
                    ).trigger(once=True).start()
                    try:
                        assert query.awaitTermination(120), "Typed collection state query did not complete"
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


def test_typed_initial_state_populates_state_online_and_generated(spark, tmp_path, integration_shared_dir) -> None:
    generated_package = "integration_v11_typed_initial_state_generated"
    files = render_generated_projects(
        ((AccumulateTypedInitialTotals, f"{SOURCE_MODULE}.AccumulateTypedInitialTotals"),),
        generated_package=generated_package,
        source_schema_modules={SOURCE_MODULE: [Event, InitialSeed, TotalOutput]},
    )

    with TemporaryDirectory(prefix=f"typed-initial-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, generated_package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                (source / "event.json").write_text(
                    json.dumps({"customer_id": "c-1", "amount": 2}) + "\n",
                    encoding="utf-8",
                )
                events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                initial = spark.createDataFrame(
                    [("c-1", 5)], "customer_id STRING NOT NULL, total INT NOT NULL"
                )
                result = AccumulateTypedInitialTotals(events=events, initial=initial).run(
                    session(spark, execution_mode=mode, generated_package=generated_package)
                )
                emitted: list[tuple[str, int]] = []

                def collect_batch(frame, _batch_id: int) -> None:
                    emitted.extend((row.customer_id, row.total) for row in frame.collect())

                query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                    "checkpointLocation", str(root / mode / "checkpoint")
                ).trigger(once=True).start()
                try:
                    assert query.awaitTermination(120), "Typed initial-state query did not complete"
                finally:
                    query.stop()

                assert emitted == [("c-1", 7)]


def test_typed_value_state_ttl_expires_after_idle_checkpoint_restart(spark, tmp_path, integration_shared_dir) -> None:
    generated_package = "integration_v11_ttl_state_generated"
    files = render_generated_projects(
        ((AccumulateExpiringTotals, f"{SOURCE_MODULE}.AccumulateExpiringTotals"),),
        generated_package=generated_package,
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )

    with TemporaryDirectory(prefix=f"ttl-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, generated_package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                checkpoint = root / mode / "checkpoint"
                emitted: list[tuple[str, int]] = []

                def run_available_now() -> None:
                    events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                    result = AccumulateExpiringTotals(events=events).run(
                        session(spark, execution_mode=mode, generated_package=generated_package)
                    )

                    def collect_batch(frame, _batch_id: int) -> None:
                        emitted.extend((row.customer_id, row.total) for row in frame.collect())

                    query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                        "checkpointLocation", str(checkpoint)
                    ).trigger(once=True).start()
                    try:
                        assert query.awaitTermination(120), "TTL state query did not complete"
                    finally:
                        query.stop()

                (source / "first.json").write_text(
                    json.dumps({"customer_id": "c-1", "amount": 3}) + "\n", encoding="utf-8"
                )
                run_available_now()
                sleep(3)
                (source / "second.json").write_text(
                    json.dumps({"customer_id": "c-1", "amount": 4}) + "\n", encoding="utf-8"
                )
                run_available_now()

                assert sorted(emitted) == [("c-1", 3), ("c-1", 4)]


def test_row_state_append_none_and_composite_keys_online_and_generated(spark, tmp_path, integration_shared_dir) -> None:
    generated_package = "integration_v11_row_state_append_generated"
    files = render_generated_projects(
        ((AccumulateAppendTotals, f"{SOURCE_MODULE}.AccumulateAppendTotals"),),
        generated_package=generated_package,
        source_schema_modules={SOURCE_MODULE: [CompositeEvent, CompositeOutput]},
    )
    with TemporaryDirectory(prefix=f"row-state-append-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, generated_package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                (source / "events.json").write_text(
                    json.dumps({"customer_id": "c-1", "region": "west", "amount": 2})
                    + "\n"
                    + json.dumps({"customer_id": "c-1", "region": "east", "amount": 5})
                    + "\n"
                    + json.dumps({"customer_id": "c-1", "region": "west", "amount": 1})
                    + "\n",
                    encoding="utf-8",
                )
                events = spark.readStream.schema(
                    "customer_id STRING NOT NULL, region STRING NOT NULL, amount INT NOT NULL"
                ).json(str(source))
                result = AccumulateAppendTotals(events=events).run(
                    session(spark, execution_mode=mode, generated_package=generated_package)
                )
                emitted: list[tuple[str, str, int]] = []

                def collect_batch(frame, _batch_id: int) -> None:
                    emitted.extend((row.customer_id, row.region, row.total) for row in frame.collect())

                query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("append").option(
                    "checkpointLocation", str(root / mode / "checkpoint")
                ).trigger(once=True).start()
                try:
                    assert query.awaitTermination(120), "Append row state query did not complete"
                finally:
                    query.stop()

                assert sorted(emitted) == [("c-1", "east", 5), ("c-1", "west", 3)]


def test_row_state_event_time_timer_fires_after_watermark_advances_online_and_generated(
    spark, tmp_path, integration_shared_dir
) -> None:
    generated_package = "integration_v11_row_state_event_time_generated"
    files = render_generated_projects(
        (
            (WatermarkEventTime, f"{SOURCE_MODULE}.WatermarkEventTime"),
            (EmitEventTimeTotals, f"{SOURCE_MODULE}.EmitEventTimeTotals"),
        ),
        generated_package=generated_package,
        source_schema_modules={SOURCE_MODULE: [EventTimeEvent, TotalOutput]},
    )
    with TemporaryDirectory(prefix=f"row-state-event-time-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, generated_package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                emitted: list[tuple[str, int]] = []
                events = spark.readStream.schema(
                    "customer_id STRING NOT NULL, event_time TIMESTAMP NOT NULL, amount INT NOT NULL"
                ).json(str(source))
                runtime = session(spark, execution_mode=mode, generated_package=generated_package)
                watermarked = WatermarkEventTime(events=events).run(runtime).watermarked
                result = EmitEventTimeTotals(events=watermarked).run(runtime)

                def collect_batch(frame, _batch_id: int) -> None:
                    emitted.extend((row.customer_id, row.total) for row in frame.collect())

                query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                    "checkpointLocation", str(root / mode / "checkpoint")
                ).trigger(processingTime="100 milliseconds").start()
                try:
                    (source / "first.json").write_text(
                        json.dumps(
                            {"customer_id": "c-1", "event_time": "2024-01-01T00:00:00", "amount": 3}
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    deadline = monotonic() + 30
                    while not emitted and monotonic() < deadline and query.isActive:
                        sleep(0.1)
                    assert not emitted, "Event-time processor emitted before timer expiry"
                    (source / "advance.json").write_text(
                        json.dumps(
                            {"customer_id": "advance", "event_time": "2024-01-01T00:00:20", "amount": 0}
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    deadline = monotonic() + 30
                    while not emitted and monotonic() < deadline and query.isActive:
                        sleep(0.1)
                    assert emitted, "Event-time timer did not fire after watermark advanced"
                finally:
                    query.stop()

                assert emitted == [("c-1", 3)]


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
