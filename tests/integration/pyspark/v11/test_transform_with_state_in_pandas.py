from __future__ import annotations

import json
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
    PandasStateProcessor,
    Timer,
    TimerContext,
    ValueState,
    external_state_processor,
    integer,
    long,
    pandas_state_processor,
    string,
    transform_with_state_in_pandas,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark40", "pyspark41"},
        reason="Pandas transformWithState evidence targets ordinary PySpark 4.0 and 4.1",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_transform_with_state_in_pandas"
GENERATED_PACKAGE = "integration_v11_pandas_state_generated"


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


class CompositeEvent(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)
    item_id = string(nullable=False)
    amount = integer(nullable=False)


class CompositeKey(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)


class InitialPandasTotal(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)
    seed_total = long(nullable=False)


class NativePandasOutput(Schema):
    customer_id = string(nullable=False)
    region = string(nullable=False)
    total = long(nullable=False)
    amount_count = integer(nullable=False)
    item_total = long(nullable=False)
    reason = string(nullable=False)


@pandas_state_processor
class CustomerTotals(PandasStateProcessor[Event, CustomerKey, CustomerTotal, TotalOutput]):
    def on_batches(self, key, batches, state: ValueState[CustomerTotal], timers):
        import pandas as pd  # type: ignore[import-untyped]

        current = state.get()
        total = 0 if current is None else current.total
        for batch in batches:
            total += int(batch["amount"].sum())
        state.update(CustomerTotal(total=total))
        yield pd.DataFrame({"customer_id": [key.customer_id], "total": [total]})


@pandas_state_processor
class TimerPandasTotals(PandasStateProcessor[Event, CustomerKey, CustomerTotal, TotalOutput]):
    def on_batches(self, key, batches, state: ValueState[CustomerTotal], timers: TimerContext):
        import pandas as pd  # type: ignore[import-untyped]

        total = sum(int(batch["amount"].sum()) for batch in batches)
        state.update(CustomerTotal(total=total))
        current = timers.current_processing_time_ms
        assert current is not None
        timers.register(current + 100)
        yield pd.DataFrame({"customer_id": [key.customer_id], "total": [total]})

    def on_timer(
        self, key: CustomerKey, timer: Timer, state: ValueState[CustomerTotal], timers: TimerContext
    ):
        import pandas as pd  # type: ignore[import-untyped]

        current = state.get()
        if current is not None:
            yield pd.DataFrame({"customer_id": [key.customer_id], "total": [current.total]})


@pandas_state_processor
class MultiplePandasOutputs(PandasStateProcessor[Event, CustomerKey, CustomerTotal, TotalOutput]):
    def on_batches(self, key, batches, state: ValueState[CustomerTotal], timers: TimerContext):
        import pandas as pd  # type: ignore[import-untyped]

        total = sum(int(batch["amount"].sum()) for batch in batches)
        state.update(CustomerTotal(total=total))
        if key.customer_id == "c-zero":
            return
        yield pd.DataFrame()
        yield pd.DataFrame({"customer_id": [key.customer_id], "total": [total]})
        yield pd.DataFrame({"customer_id": [key.customer_id], "total": [total + 1]})


class AccumulateCustomerTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state_in_pandas(
            key=event.customer_id,
            processor=CustomerTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


class EmitPandasTimerTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state_in_pandas(
            key=event.customer_id,
            processor=TimerPandasTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


class EmitMultiplePandasFrames(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state_in_pandas(
            key=event.customer_id,
            processor=MultiplePandasOutputs,
            output_mode="Update",
            time_mode="ProcessingTime",
        )


class AccumulateNativePandasTotals(Transform):
    events = input(CompositeEvent, streaming=True)
    initial = input(InitialPandasTotal)
    totals = output(NativePandasOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: CompositeEvent) -> NativePandasOutput:
        from integration.pyspark.v11.native_state_processors import NativePandasCompositeTotals

        processor = external_state_processor(
            NativePandasCompositeTotals,
            input=CompositeEvent,
            key=CompositeKey,
            states=(CustomerTotal,),
            output=NativePandasOutput,
        )
        return transform_with_state_in_pandas(
            key=(event.customer_id, event.region),
            processor=processor,
            output_mode="Update",
            time_mode="ProcessingTime",
            initial_state=self.initial,
        )


def test_pandas_state_runs_online_and_generated_and_resumes_checkpoint(
    spark, tmp_path, integration_shared_dir
) -> None:
    files = render_generated_projects(
        ((AccumulateCustomerTotals, f"{SOURCE_MODULE}.AccumulateCustomerTotals"),),
        generated_package=GENERATED_PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )
    generated_source = "\n".join(files.values())
    assert "interface='pandas'" in generated_source
    assert "apply_stateful_transform(" in generated_source
    for token in ("readStream", "writeStream", "checkpointLocation", "toPandas(", ".rdd"):
        assert token not in generated_source

    with TemporaryDirectory(prefix=f"pandas-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, GENERATED_PACKAGE, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                checkpoint = root / mode / "checkpoint"
                emitted: list[tuple[str, int]] = []

                def build_transform():
                    events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                    return AccumulateCustomerTotals(events=events).run(
                        session(spark, execution_mode=mode, generated_package=GENERATED_PACKAGE)
                    )

                def run_available_now() -> None:
                    result = build_transform()

                    def collect_batch(frame, _batch_id: int) -> None:
                        emitted.extend((row.customer_id, row.total) for row in frame.collect())

                    query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                        "checkpointLocation", str(checkpoint)
                    ).trigger(once=True).start()
                    try:
                        assert query.awaitTermination(120), "Pandas state query did not complete"
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


def test_native_pandas_state_supports_composite_keys_initial_state_and_restart(
    spark, tmp_path, integration_shared_dir
) -> None:
    files = render_generated_projects(
        ((AccumulateNativePandasTotals, f"{SOURCE_MODULE}.AccumulateNativePandasTotals"),),
        generated_package=f"{GENERATED_PACKAGE}_native",
        source_schema_modules={SOURCE_MODULE: [CompositeEvent, InitialPandasTotal, NativePandasOutput]},
    )
    generated = "\n".join(files.values())
    assert "NativePandasCompositeTotals" in generated
    assert "key=(" in generated
    assert "initial_state=" in generated

    with TemporaryDirectory(prefix=f"native-pandas-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        package = f"{GENERATED_PACKAGE}_native"
        with generated_project(tmp_path, package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                checkpoint = root / mode / "checkpoint"
                emitted: list[tuple[str, str, int, int, int, str]] = []
                initial = spark.createDataFrame(
                    [("c-1", "west", 5)],
                    "customer_id STRING NOT NULL, region STRING NOT NULL, seed_total BIGINT NOT NULL",
                )

                def run_stream(expected_total: int, *, await_timer: bool) -> None:
                    events = spark.readStream.schema(
                        "customer_id STRING NOT NULL, region STRING NOT NULL, item_id STRING NOT NULL, amount INT NOT NULL"
                    ).json(str(source))
                    result = AccumulateNativePandasTotals(events=events, initial=initial).run(
                        session(spark, execution_mode=mode, generated_package=package)
                    )

                    def collect_batch(frame, _batch_id: int) -> None:
                        emitted.extend(
                            (
                                row.customer_id,
                                row.region,
                                row.total,
                                row.amount_count,
                                row.item_total,
                                row.reason,
                            )
                            for row in frame.collect()
                        )

                    start = len(emitted)
                    query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                        "checkpointLocation", str(checkpoint)
                    ).trigger(processingTime="100 milliseconds").start()
                    expected_input = (expected_total, 3 if expected_total == 8 else 4, 3 if expected_total == 8 else 7, "input")
                    expected_timer = (expected_total, 3, 3, "timer")
                    deadline = monotonic() + 30
                    try:
                        while not (
                            any(row[2:] == expected_input for row in emitted[start:])
                            and (not await_timer or any(row[2:] == expected_timer for row in emitted[start:]))
                        ) and monotonic() < deadline:
                            sleep(0.1)
                        assert any(row[2:] == expected_input for row in emitted[start:]), (
                            f"native Pandas input did not produce total {expected_total}"
                        )
                        if await_timer:
                            assert any(row[2:] == expected_timer for row in emitted[start:]), (
                                "native Pandas processor timer did not fire"
                            )
                    finally:
                        query.stop()

                (source / "first.json").write_text(
                    json.dumps({"customer_id": "c-1", "region": "west", "item_id": "i-1", "amount": 2})
                    + "\n"
                    + json.dumps({"customer_id": "c-1", "region": "west", "item_id": "i-1", "amount": 1})
                    + "\n",
                    encoding="utf-8",
                )
                run_stream(8, await_timer=True)
                assert any(row[2:] == (8, 3, 3, "input") for row in emitted)
                assert any(row[2:] == (8, 3, 3, "timer") for row in emitted)

                (source / "second.json").write_text(
                    json.dumps({"customer_id": "c-1", "region": "west", "item_id": "i-1", "amount": 4}) + "\n",
                    encoding="utf-8",
                )
                before_resume = len(emitted)
                run_stream(12, await_timer=False)
                assert any(row[2:] == (12, 4, 7, "input") for row in emitted[before_resume:])


def test_direct_pyspark_pandas_processor_accepts_native_operator_arguments(spark, integration_shared_dir) -> None:
    from integration.pyspark.v11.native_state_processors import NativePandasCompositeTotals
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    with TemporaryDirectory(prefix=f"direct-pandas-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        source = root / "source"
        source.mkdir(parents=True)
        (source / "event.json").write_text(
            json.dumps({"customer_id": "c-1", "region": "west", "item_id": "i-1", "amount": 2}) + "\n",
            encoding="utf-8",
        )
        events = spark.readStream.schema(
            "customer_id STRING NOT NULL, region STRING NOT NULL, item_id STRING NOT NULL, amount INT NOT NULL"
        ).json(str(source))
        initial = spark.createDataFrame(
            [("c-1", "west", 5)],
            "customer_id STRING NOT NULL, region STRING NOT NULL, seed_total BIGINT NOT NULL",
        )
        output_schema = StructType(
            [
                StructField("customer_id", StringType(), nullable=False),
                StructField("region", StringType(), nullable=False),
                StructField("total", LongType(), nullable=False),
                StructField("amount_count", LongType(), nullable=False),
                StructField("item_total", LongType(), nullable=False),
                StructField("reason", StringType(), nullable=False),
            ]
        )
        result = events.groupBy("customer_id", "region").transformWithStateInPandas(
            statefulProcessor=NativePandasCompositeTotals(),
            outputStructType=output_schema,
            outputMode="Update",
            timeMode="ProcessingTime",
            initialState=initial.groupBy("customer_id", "region"),
            eventTimeColumnName="",
        )
        emitted: list[tuple[str, str, int, int, int, str]] = []

        def collect_batch(frame, _batch_id: int) -> None:
            emitted.extend(
                (row.customer_id, row.region, row.total, row.amount_count, row.item_total, row.reason)
                for row in frame.collect()
            )

        query = result.writeStream.foreachBatch(collect_batch).outputMode("update").option(
            "checkpointLocation", str(root / "checkpoint")
        ).trigger(processingTime="100 milliseconds").start()
        deadline = monotonic() + 30
        try:
            while not (
                any(row[2:] == (7, 2, 2, "input") for row in emitted)
                and any(row[2:] == (7, 2, 2, "timer") for row in emitted)
            ) and monotonic() < deadline:
                sleep(0.1)
            assert any(row[2:] == (7, 2, 2, "input") for row in emitted), "direct native Pandas input was not processed"
            assert any(row[2:] == (7, 2, 2, "timer") for row in emitted), "direct native Pandas timer did not fire"
        finally:
            query.stop()


def test_typed_pandas_state_emits_multiple_frames_and_allows_zero_output(spark, tmp_path, integration_shared_dir) -> None:
    files = render_generated_projects(
        ((EmitMultiplePandasFrames, f"{SOURCE_MODULE}.EmitMultiplePandasFrames"),),
        generated_package=f"{GENERATED_PACKAGE}_outputs",
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )
    package = f"{GENERATED_PACKAGE}_outputs"
    with TemporaryDirectory(prefix=f"pandas-outputs-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                (source / "events.json").write_text(
                    json.dumps({"customer_id": "c-one", "amount": 2})
                    + "\n"
                    + json.dumps({"customer_id": "c-zero", "amount": 3})
                    + "\n",
                    encoding="utf-8",
                )
                events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                result = EmitMultiplePandasFrames(events=events).run(
                    session(spark, execution_mode=mode, generated_package=package)
                )
                emitted: list[tuple[str, int]] = []

                def collect_batch(frame, _batch_id: int) -> None:
                    emitted.extend((row.customer_id, row.total) for row in frame.collect())

                query = result.totals.writeStream.foreachBatch(collect_batch).outputMode("update").option(
                    "checkpointLocation", str(root / mode / "checkpoint")
                ).trigger(once=True).start()
                try:
                    assert query.awaitTermination(120), "Pandas multi-frame query did not complete"
                finally:
                    query.stop()
                assert sorted(emitted) == [("c-one", 2), ("c-one", 3)]


def test_typed_pandas_timer_callback_runs_online_and_generated(spark, tmp_path, integration_shared_dir) -> None:
    files = render_generated_projects(
        ((EmitPandasTimerTotals, f"{SOURCE_MODULE}.EmitPandasTimerTotals"),),
        generated_package=f"{GENERATED_PACKAGE}_timer",
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )
    package = f"{GENERATED_PACKAGE}_timer"
    with TemporaryDirectory(prefix=f"pandas-timer-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)
        with generated_project(tmp_path, package, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                (source / "events.json").write_text(
                    json.dumps({"customer_id": "c-1", "amount": 3}) + "\n", encoding="utf-8"
                )
                events = spark.readStream.schema("customer_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
                result = EmitPandasTimerTotals(events=events).run(
                    session(spark, execution_mode=mode, generated_package=package)
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
                    assert len(emitted) >= 2, "typed Pandas timer did not emit"
                finally:
                    query.stop()
                assert sorted(emitted) == [("c-1", 3), ("c-1", 3)]
