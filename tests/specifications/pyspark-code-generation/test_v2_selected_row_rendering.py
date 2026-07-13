from typing import Any, cast

import pytest

import structure
from structure.app.cli.commands.RenderExplainReport import render_explain_report
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step
from structure.app.target.pyspark.commands.RenderPySparkTransformModule import render_pyspark_transform_module


class RawEvent(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    sequence = structure.field(structure.Long(), nullable=False)


class LatestEvent(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    sequence = structure.field(structure.Long(), nullable=False)


class Account(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False, primary_key=True)
    tier = structure.field(structure.String(), nullable=False)


class AccountEvent(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    tier = structure.field(structure.String(), nullable=True)


class RankedEvent(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    sequence = structure.field(structure.Long(), nullable=False)
    row_number = structure.field(structure.Long(), nullable=False)
    rank = structure.field(structure.Long(), nullable=False)
    dense_rank = structure.field(structure.Long(), nullable=False)
    previous_sequence = structure.field(structure.Long(), nullable=True)
    next_sequence = structure.field(structure.Long(), nullable=True)
    rolling_units = structure.field(structure.Long(), nullable=False)
    rolling_avg_units = structure.field(structure.Double(), nullable=False)
    rolling_min_units = structure.field(structure.Long(), nullable=False)
    rolling_max_units = structure.field(structure.Long(), nullable=False)


class AdvancedRankedEvent(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    percent_rank = structure.field(structure.Double(), nullable=False)
    cume_dist = structure.field(structure.Double(), nullable=False)
    bucket = structure.field(structure.Long(), nullable=False)
    framed_total = structure.field(structure.Long(), nullable=False)


class MultiOrderedEvent(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    sequence = structure.field(structure.Long(), nullable=False)
    rank = structure.field(structure.Long(), nullable=False)
    running_total = structure.field(structure.Long(), nullable=False)


class AggregateWindowEvent(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    accepted = structure.field(structure.Boolean(), nullable=True)
    sequence_stddev = structure.field(structure.Double(), nullable=True)
    sequence_variance = structure.field(structure.Double(), nullable=True)
    sequences = structure.field(structure.Array(structure.Long(), contains_null=False), nullable=True)
    distinct_sequences = structure.field(structure.Array(structure.Long(), contains_null=False), nullable=True)


@structure.transform
class LatestEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    latest = structure.output(LatestEvent)

    def latest_events(self, row: RawEvent) -> LatestEvent:
        structure.latest_by(row.sequence, partition_by=row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@structure.transform
class LatestDedupeEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    latest = structure.output(LatestEvent)

    def latest_events(self, row: RawEvent) -> LatestEvent:
        structure.dedupe_latest_by(row.sequence, partition_by=row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@structure.transform
class EarliestDedupeEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    earliest = structure.output(LatestEvent)

    def earliest_events(self, row: RawEvent) -> LatestEvent:
        structure.dedupe_earliest_by(row.sequence, partition_by=row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@structure.transform
class RankedEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    ranked = structure.output(RankedEvent)

    def rank_events(self, row: RawEvent) -> RankedEvent:
        event = cast(Any, row)
        return RankedEvent(
            account_id=row.account_id,
            event_id=row.event_id,
            sequence=row.sequence,
            row_number=structure.row_number(partition_by=row.account_id, order_by=event.sequence.asc()),
            rank=structure.rank(partition_by=row.account_id, order_by=event.sequence.desc()),
            dense_rank=structure.dense_rank(partition_by=row.account_id, order_by=row.sequence),
            previous_sequence=structure.lag(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            next_sequence=structure.lead(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            rolling_units=structure.rolling_sum(
                row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_avg_units=structure.rolling_avg(
                row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_min_units=structure.rolling_min(
                row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_max_units=structure.rolling_max(
                row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
        )


@structure.transform
class AdvancedRankedEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    ranked = structure.output(AdvancedRankedEvent)

    def rank_events(self, row: RawEvent) -> AdvancedRankedEvent:
        event = cast(Any, row)
        spec = structure.window(
            partition_by=row.account_id,
            order_by=event.sequence.asc_nulls_last(),
            frame=structure.rows_between(structure.preceding(3), structure.current_row()),
        )
        return AdvancedRankedEvent(
            account_id=row.account_id,
            percent_rank=structure.percent_rank(over=spec),
            cume_dist=structure.cume_dist(over=spec),
            bucket=structure.ntile(4, over=spec),
            framed_total=structure.window_sum(row.sequence, over=spec),
        )


@structure.transform
class MultiOrderedEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    ranked = structure.output(MultiOrderedEvent)

    def rank_events(self, row: RawEvent) -> MultiOrderedEvent:
        event = cast(Any, row)
        spec = structure.window(
            partition_by=row.account_id,
            order_by=(event.sequence.asc_nulls_last(), event.event_id.desc_nulls_first()),
            frame=structure.rows_between(structure.preceding(2), structure.current_row()),
        )
        return MultiOrderedEvent(
            account_id=row.account_id,
            event_id=row.event_id,
            sequence=row.sequence,
            rank=structure.rank(
                partition_by=row.account_id,
                order_by=(event.sequence.asc_nulls_last(), event.event_id.desc_nulls_first()),
            ),
            running_total=structure.window_sum(row.sequence, over=spec),
        )


@structure.transform
class AggregateWindowEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    aggregated = structure.output(AggregateWindowEvent)

    def aggregate_events(self, row: RawEvent) -> AggregateWindowEvent:
        event = cast(Any, row)
        spec = structure.window(
            partition_by=row.account_id,
            order_by=row.sequence,
            frame=structure.rows_between(structure.unbounded_preceding(), structure.current_row()),
        )
        return AggregateWindowEvent(
            account_id=row.account_id,
            accepted=structure.window_bool_and(event.sequence > 0, over=spec),
            sequence_stddev=structure.window_stddev(row.sequence, over=spec),
            sequence_variance=structure.window_variance(row.sequence, over=spec),
            sequences=structure.window_collect_list(row.sequence, over=spec),
            distinct_sequences=structure.window_collect_set(row.sequence, over=spec),
        )


@structure.transform
class UniqueEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    unique = structure.output(LatestEvent)

    def unique_events(self, row: RawEvent) -> LatestEvent:
        structure.distinct()
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@structure.transform
class UniqueAccountEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    unique = structure.output(LatestEvent)

    def unique_events(self, row: RawEvent) -> LatestEvent:
        structure.drop_duplicates(row.account_id)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@structure.transform
class UniqueRelationEventTransform(structure.Transform):
    events = structure.input(RawEvent)
    unique = structure.output(LatestEvent)

    def unique_events(self, row: RawEvent) -> LatestEvent:
        structure.distinct(row)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@structure.transform
class PreJoinUniqueAccountTransform(structure.Transform):
    events = structure.input(RawEvent)
    accounts = structure.input(Account)
    enriched = structure.output(AccountEvent)

    def enrich(self, event: RawEvent, account: Account) -> AccountEvent:
        structure.drop_duplicates(account.account_id)
        structure.lookup_join(account, on=account.account_id == event.account_id)
        return AccountEvent(account_id=event.account_id, event_id=event.event_id, tier=account.tier)


@structure.transform
class PostJoinUniqueAccountTransform(structure.Transform):
    events = structure.input(RawEvent)
    accounts = structure.input(Account)
    enriched = structure.output(AccountEvent)

    def enrich(self, event: RawEvent, account: Account) -> AccountEvent:
        structure.lookup_join(account, on=account.account_id == event.account_id)
        structure.drop_duplicates(account.account_id)
        return AccountEvent(account_id=event.account_id, event_id=event.event_id, tier=account.tier)


@structure.transform
class MixedScopeDropDuplicatesTransform(structure.Transform):
    events = structure.input(RawEvent)
    accounts = structure.input(Account)
    enriched = structure.output(AccountEvent)

    def enrich(self, event: RawEvent, account: Account) -> AccountEvent:
        structure.lookup_join(account, on=account.account_id == event.account_id)
        structure.drop_duplicates(event.account_id, account.account_id)
        return AccountEvent(account_id=event.account_id, event_id=event.event_id, tier=account.tier)


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


def test_advanced_window_helpers_render_valid_function_frames() -> None:
    plan = PySpark.plan.lower()(compile_transform(AdvancedRankedEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert (
        'F.percent_rank().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc_nulls_last()))'
    ) in text
    assert (
        'F.cume_dist().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc_nulls_last()))'
    ) in text
    assert (
        'F.ntile(4).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc_nulls_last()))'
    ) in text
    assert (
        'F.sum(F.col("raw_event.sequence")).over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").asc_nulls_last()).rowsBetween(-3, Window.currentRow))'
    ) in text


def test_window_helpers_render_multiple_explicitly_ordered_keys() -> None:
    plan = PySpark.plan.lower()(compile_transform(MultiOrderedEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    order = 'orderBy(F.col("raw_event.sequence").asc_nulls_last(), F.col("raw_event.event_id").desc_nulls_first())'
    assert f"F.rank().over(Window.partitionBy(F.col(\"raw_event.account_id\")).{order})" in text
    assert f"F.sum(F.col(\"raw_event.sequence\")).over(Window.partitionBy(F.col(\"raw_event.account_id\")).{order}.rowsBetween(-2, Window.currentRow))" in text


def test_window_requires_at_least_one_order_key() -> None:
    with pytest.raises(TypeError, match="window\\(\\.\\.\\.\\) requires at least one order_by expression"):
        structure.window(partition_by=RawEvent.account_id, order_by=())


def test_window_aggregate_helpers_render_over_an_explicit_frame() -> None:
    plan = PySpark.plan.lower()(compile_transform(AggregateWindowEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    window = (
        'Window.partitionBy(F.col("raw_event.account_id")).orderBy(F.col("raw_event.sequence").asc()).'
        'rowsBetween(Window.unboundedPreceding, Window.currentRow)'
    )
    assert f"F.bool_and((F.col(\"raw_event.sequence\") > F.lit(0))).over({window})" in text
    assert f"F.stddev(F.col(\"raw_event.sequence\")).over({window})" in text
    assert f"F.variance(F.col(\"raw_event.sequence\")).over({window})" in text
    assert f"F.collect_list(F.col(\"raw_event.sequence\")).over({window})" in text
    assert f"F.collect_set(F.col(\"raw_event.sequence\")).over({window})" in text


def test_window_aggregate_helpers_reject_invalid_inputs_and_combinations() -> None:
    spec = structure.window(partition_by=RawEvent.account_id, order_by=RawEvent.sequence)

    with pytest.raises(TypeError, match="window_bool_and\\(\\.\\.\\.\\) requires a Boolean expression"):
        structure.window_bool_and(RawEvent.sequence, over=spec)
    with pytest.raises(TypeError, match="window_stddev\\(\\.\\.\\.\\) requires a numeric expression"):
        structure.window_stddev(RawEvent.event_id, over=spec)
    with pytest.raises(TypeError, match="does not permit distinct window aggregates"):
        structure.window_count_distinct(RawEvent.sequence, over=spec)
    with pytest.raises(TypeError, match="range_between\\(\\.\\.\\.\\) requires exactly one order_by expression"):
        structure.window(
            partition_by=RawEvent.account_id,
            order_by=(RawEvent.sequence, RawEvent.event_id),
            frame=structure.range_between(structure.preceding(1), structure.current_row()),
        )


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


def test_distinct_relation_renders_exact_relation_duplicate_removal() -> None:
    plan = PySpark.plan.lower()(compile_transform(UniqueRelationEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert 'events = events.dropDuplicates(["account_id", "event_id", "sequence"])' in text


def test_drop_duplicates_explain_names_dedupe_operation_and_streaming_status() -> None:
    text = render_explain_report(UniqueEventTransform)

    assert "operations: drop_duplicates(row_filtering streaming_modes=append)" in text
    assert "STREAM-E0801: batch_only in unique_events (exact duplicate removal)" in text


def test_drop_duplicates_renders_subset_columns_when_requested() -> None:
    plan = PySpark.plan.lower()(compile_transform(UniqueAccountEventTransform))

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert 'events = events.dropDuplicates(["account_id"])' in text


def test_drop_duplicates_subset_explain_names_subset_and_streaming_status() -> None:
    text = render_explain_report(UniqueAccountEventTransform)

    assert "operations: drop_duplicates(row_filtering subset=1 scope=events streaming_modes=append)" in text
    assert "STREAM-E0801: batch_only in unique_events (subset duplicate removal)" in text


def test_relation_drop_duplicates_before_join_prepares_join_source() -> None:
    plan = PySpark.plan.lower()(compile_transform(PreJoinUniqueAccountTransform))

    text = render_pyspark_step(
        plan.steps[0],
        current="events",
        sources={"events": "events", "accounts": "accounts"},
    )

    assert 'events_account_deduped_1 = accounts.dropDuplicates(["account_id"])' in text
    assert text.index("events_account_deduped_1 =") < text.index("events = events.join(")


def test_relation_drop_duplicates_after_join_applies_to_joined_frame() -> None:
    plan = PySpark.plan.lower()(compile_transform(PostJoinUniqueAccountTransform))

    text = render_pyspark_step(
        plan.steps[0],
        current="events",
        sources={"events": "events", "accounts": "accounts"},
    )

    assert 'events = events.dropDuplicates(["account_id"])' in text
    assert text.index("events = events.join(") < text.index('events = events.dropDuplicates(["account_id"])')


def test_drop_duplicates_rejects_mixed_relation_scopes() -> None:
    with pytest.raises(TypeError, match="one relation scope"):
        compile_transform(MixedScopeDropDuplicatesTransform)
