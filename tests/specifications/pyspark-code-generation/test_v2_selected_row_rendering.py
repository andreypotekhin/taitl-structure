from structure import (
    Long,
    String,
    Structure,
    Transform,
    distinct,
    drop_duplicates,
    field,
    input,
    latest_by,
    output,
    transform,
)
from structure.app.cli.commands.RenderExplainReport import render_explain_report
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class RawEvent(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    sequence = field(Long(), nullable=False)


class LatestEvent(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    sequence = field(Long(), nullable=False)


@transform
class LatestEventTransform(Transform):
    events = input(RawEvent)
    latest = output(LatestEvent)

    def latest_events(self, row: RawEvent) -> LatestEvent:
        latest_by(row.sequence, partition_by=row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


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
