from structure import (
    Double,
    Long,
    String,
    Structure,
    Transform,
    dedupe_earliest_by,
    dedupe_latest_by,
    dense_rank,
    distinct,
    drop_duplicates,
    field,
    input,
    lag,
    latest_by,
    lead,
    output,
    rank,
    rolling_avg,
    rolling_max,
    rolling_min,
    rolling_sum,
    row_number,
    transform,
)
from structure.app.cli.commands.RenderExplainReport import render_explain_report
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step
from structure.app.target.pyspark.commands.RenderPySparkTransformModule import render_pyspark_transform_module


class RawEvent(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    sequence = field(Long(), nullable=False)


class LatestEvent(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    sequence = field(Long(), nullable=False)


class RankedEvent(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    sequence = field(Long(), nullable=False)
    row_number = field(Long(), nullable=False)
    rank = field(Long(), nullable=False)
    dense_rank = field(Long(), nullable=False)
    previous_sequence = field(Long(), nullable=True)
    next_sequence = field(Long(), nullable=True)
    rolling_units = field(Long(), nullable=False)
    rolling_avg_units = field(Double(), nullable=False)
    rolling_min_units = field(Long(), nullable=False)
    rolling_max_units = field(Long(), nullable=False)


@transform
class LatestEventTransform(Transform):
    events = input(RawEvent)
    latest = output(LatestEvent)

    def latest_events(self, row: RawEvent) -> LatestEvent:
        latest_by(row.sequence, partition_by=row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@transform
class LatestDedupeEventTransform(Transform):
    events = input(RawEvent)
    latest = output(LatestEvent)

    def latest_events(self, row: RawEvent) -> LatestEvent:
        dedupe_latest_by(row.sequence, partition_by=row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@transform
class EarliestDedupeEventTransform(Transform):
    events = input(RawEvent)
    earliest = output(LatestEvent)

    def earliest_events(self, row: RawEvent) -> LatestEvent:
        dedupe_earliest_by(row.sequence, partition_by=row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@transform
class RankedEventTransform(Transform):
    events = input(RawEvent)
    ranked = output(RankedEvent)

    def rank_events(self, row: RawEvent) -> RankedEvent:
        return RankedEvent(
            account_id=row.account_id,
            event_id=row.event_id,
            sequence=row.sequence,
            row_number=row_number(partition_by=row.account_id, order_by=row.sequence),
            rank=rank(partition_by=row.account_id, order_by=row.sequence, descending=True),
            dense_rank=dense_rank(partition_by=row.account_id, order_by=row.sequence),
            previous_sequence=lag(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            next_sequence=lead(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            rolling_units=rolling_sum(row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2),
            rolling_avg_units=rolling_avg(row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2),
            rolling_min_units=rolling_min(row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2),
            rolling_max_units=rolling_max(row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2),
        )


@transform
class UniqueEventTransform(Transform):
    events = input(RawEvent)
    unique = output(LatestEvent)

    def unique_events(self, row: RawEvent) -> LatestEvent:
        distinct()
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@transform
class UniqueAccountEventTransform(Transform):
    events = input(RawEvent)
    unique = output(LatestEvent)

    def unique_events(self, row: RawEvent) -> LatestEvent:
        drop_duplicates(row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


def test_latest_by_renders_spark_visible_row_number_window() -> None:
    plan = PySpark.plan.lower()(compile_transform(LatestEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert (
        'events = events.withColumn("__structure_latest_events_latest_rank", '
        'F.row_number().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").desc())))'
    ) in text
    assert 'events = events.where(F.col("__structure_latest_events_latest_rank") == F.lit(1))' in text
    assert 'events = events.drop("__structure_latest_events_latest_rank")' in text


def test_dedupe_latest_by_renders_deterministic_selected_row_window() -> None:
    plan = PySpark.plan.lower()(compile_transform(LatestDedupeEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert (
        'events = events.withColumn("__structure_latest_events_latest_rank", '
        'F.row_number().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").desc())))'
    ) in text
    assert 'events = events.where(F.col("__structure_latest_events_latest_rank") == F.lit(1))' in text


def test_dedupe_earliest_by_records_selected_row_operation() -> None:
    plan = PySpark.plan.lower()(compile_transform(EarliestDedupeEventTransform))
    operation = plan.steps[0].operations[0]

    assert operation.selected_rows is not None
    assert operation.selected_rows.direction == "earliest"
    assert operation.selected_rows.partition_by[0].data is not None
    assert operation.selected_rows.partition_by[0].data["field"] == "account_id"


def test_window_projection_helpers_render_spark_visible_windows() -> None:
    plan = PySpark.plan.lower()(compile_transform(RankedEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert (
        'F.row_number().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc())).cast(T.LongType()).alias("row_number")'
    ) in text
    assert (
        'F.rank().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").desc())).cast(T.LongType()).alias("rank")'
    ) in text
    assert (
        'F.dense_rank().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc())).cast(T.LongType()).alias("dense_rank")'
    ) in text
    assert (
        'F.lag(F.col("raw_event.sequence"), 1).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc())).alias("previous_sequence")'
    ) in text
    assert (
        'F.lead(F.col("raw_event.sequence"), 1).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc())).alias("next_sequence")'
    ) in text
    assert (
        'F.sum(F.col("raw_event.sequence")).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc()).rowsBetween(-2, 0)).alias("rolling_units")'
    ) in text
    assert (
        'F.avg(F.col("raw_event.sequence")).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc()).rowsBetween(-2, 0)).alias("rolling_avg_units")'
    ) in text
    assert (
        'F.min(F.col("raw_event.sequence")).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc()).rowsBetween(-2, 0)).alias("rolling_min_units")'
    ) in text
    assert (
        'F.max(F.col("raw_event.sequence")).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc()).rowsBetween(-2, 0)).alias("rolling_max_units")'
    ) in text


def test_window_projection_helpers_add_window_import_to_generated_module() -> None:
    plan = PySpark.plan.lower()(compile_transform(RankedEventTransform))

    text = render_pyspark_transform_module(
        plan,
        source_transform="tests.RankedEventTransform",
        schema_modules={RawEvent: "tests.schemas", RankedEvent: "tests.schemas"},
        runtime_module="tests.runtime",
    )

    assert "from pyspark.sql import Window" in text


def test_window_projection_helpers_are_batch_only_in_explain() -> None:
    text = render_explain_report(RankedEventTransform)

    assert "STREAM-E0801: batch_only in rank_events (window projection)" in text


def test_latest_by_explain_names_window_operation_and_streaming_status() -> None:
    text = render_explain_report(LatestEventTransform)

    assert "operations: latest_by(select_one partitions=1)" in text
    assert "STREAM-E0801: batch_only in latest_events (latest-row selection)" in text


def test_drop_duplicates_renders_spark_visible_exact_duplicate_removal() -> None:
    plan = PySpark.plan.lower()(compile_transform(UniqueEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert "events = events.dropDuplicates()" in text


def test_drop_duplicates_explain_names_dedupe_operation_and_streaming_status() -> None:
    text = render_explain_report(UniqueEventTransform)

    assert "operations: drop_duplicates(row_filtering)" in text
    assert "STREAM-E0801: batch_only in unique_events (exact duplicate removal)" in text


def test_drop_duplicates_renders_subset_columns_when_requested() -> None:
    plan = PySpark.plan.lower()(compile_transform(UniqueAccountEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert 'events = events.dropDuplicates(["account_id"])' in text


def test_drop_duplicates_subset_explain_names_subset_and_streaming_status() -> None:
    text = render_explain_report(UniqueAccountEventTransform)

    assert "operations: drop_duplicates(row_filtering subset=1)" in text
    assert "STREAM-E0801: batch_only in unique_events (subset duplicate removal)" in text
