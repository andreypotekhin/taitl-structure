from typing import Any, cast

import pytest

from structure import *
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module


def _compile(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def _recipe(transform) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, _compile(transform).lowered)


class RawEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)


class LatestEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)


class Account(Schema):
    account_id = string(nullable=False)
    tier = string(nullable=False)


class AccountEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    tier = string(nullable=True)


class RankedEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)
    row_number = long(nullable=False)
    rank = long(nullable=False)
    dense_rank = long(nullable=False)
    previous_sequence = long(nullable=True)
    next_sequence = long(nullable=True)
    rolling_units = long(nullable=False)
    rolling_avg_units = double(nullable=False)
    rolling_min_units = long(nullable=False)
    rolling_max_units = long(nullable=False)


class AdvancedRankedEvent(Schema):
    account_id = string(nullable=False)
    percent_rank = double(nullable=False)
    cume_dist = double(nullable=False)
    bucket = long(nullable=False)
    framed_total = long(nullable=False)


class MultiOrderedEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)
    rank = long(nullable=False)
    running_total = long(nullable=False)


class AggregateWindowEvent(Schema):
    account_id = string(nullable=False)
    accepted = boolean(nullable=True)
    sequence_stddev = double(nullable=True)
    sequence_variance = double(nullable=True)
    sequences = array(long(), contains_null=False, nullable=True)
    distinct_sequences = array(long(), contains_null=False, nullable=True)


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
class EarliestEventTransform(Transform):
    events = input(RawEvent)
    earliest = output(LatestEvent)

    def earliest_events(self, row: RawEvent) -> LatestEvent:
        earliest_by(row.sequence, partition_by=row.account_id)
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
        event = cast(Any, row)
        return RankedEvent(
            account_id=row.account_id,
            event_id=row.event_id,
            sequence=row.sequence,
            row_number=row_number(partition_by=row.account_id, order_by=event.sequence.asc()),
            rank=rank(partition_by=row.account_id, order_by=event.sequence.desc()),
            dense_rank=dense_rank(partition_by=row.account_id, order_by=row.sequence),
            previous_sequence=lag(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            next_sequence=lead(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            rolling_units=rolling_sum(row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2),
            rolling_avg_units=rolling_avg(
                row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_min_units=rolling_min(
                row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_max_units=rolling_max(
                row.sequence, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
        )


@transform
class AdvancedRankedEventTransform(Transform):
    events = input(RawEvent)
    ranked = output(AdvancedRankedEvent)

    def rank_events(self, row: RawEvent) -> AdvancedRankedEvent:
        event = cast(Any, row)
        spec = window(
            partition_by=row.account_id,
            order_by=event.sequence.asc_nulls_last(),
            frame=rows_between(preceding(3), current_row()),
        )
        return AdvancedRankedEvent(
            account_id=row.account_id,
            percent_rank=percent_rank(over=spec),
            cume_dist=cume_dist(over=spec),
            bucket=ntile(4, over=spec),
            framed_total=window_sum(row.sequence, over=spec),
        )


@transform
class MultiOrderedEventTransform(Transform):
    events = input(RawEvent)
    ranked = output(MultiOrderedEvent)

    def rank_events(self, row: RawEvent) -> MultiOrderedEvent:
        event = cast(Any, row)
        spec = window(
            partition_by=row.account_id,
            order_by=(event.sequence.asc_nulls_last(), event.event_id.desc_nulls_first()),
            frame=rows_between(preceding(2), current_row()),
        )
        return MultiOrderedEvent(
            account_id=row.account_id,
            event_id=row.event_id,
            sequence=row.sequence,
            rank=rank(
                partition_by=row.account_id,
                order_by=(event.sequence.asc_nulls_last(), event.event_id.desc_nulls_first()),
            ),
            running_total=window_sum(row.sequence, over=spec),
        )


@transform
class AggregateWindowEventTransform(Transform):
    events = input(RawEvent)
    aggregated = output(AggregateWindowEvent)

    def aggregate_events(self, row: RawEvent) -> AggregateWindowEvent:
        event = cast(Any, row)
        spec = window(
            partition_by=row.account_id,
            order_by=row.sequence,
            frame=rows_between(unbounded_preceding(), current_row()),
        )
        return AggregateWindowEvent(
            account_id=row.account_id,
            accepted=window_bool_and(event.sequence > 0, over=spec),
            sequence_stddev=window_stddev(row.sequence, over=spec),
            sequence_variance=window_variance(row.sequence, over=spec),
            sequences=window_collect_list(row.sequence, over=spec),
            distinct_sequences=window_collect_set(row.sequence, over=spec),
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


@transform
class UniqueRelationEventTransform(Transform):
    events = input(RawEvent)
    unique = output(LatestEvent)

    def unique_events(self, row: RawEvent) -> LatestEvent:
        distinct(row)
        return LatestEvent(account_id=row.account_id, event_id=row.event_id, sequence=row.sequence)


@transform
class PreJoinUniqueAccountTransform(Transform):
    events = input(RawEvent)
    accounts = input(Account)
    enriched = output(AccountEvent)

    def enrich(self, event: RawEvent, account: Account) -> AccountEvent:
        drop_duplicates(account.account_id)
        lookup_join(account, on=account.account_id == event.account_id)
        return AccountEvent(account_id=event.account_id, event_id=event.event_id, tier=account.tier)


@transform
class PostJoinUniqueAccountTransform(Transform):
    events = input(RawEvent)
    accounts = input(Account)
    enriched = output(AccountEvent)

    def enrich(self, event: RawEvent, account: Account) -> AccountEvent:
        lookup_join(account, on=account.account_id == event.account_id)
        drop_duplicates(account.account_id)
        return AccountEvent(account_id=event.account_id, event_id=event.event_id, tier=account.tier)


@transform
class MixedScopeDropDuplicatesTransform(Transform):
    events = input(RawEvent)
    accounts = input(Account)
    enriched = output(AccountEvent)

    def enrich(self, event: RawEvent, account: Account) -> AccountEvent:
        lookup_join(account, on=account.account_id == event.account_id)
        drop_duplicates(event.account_id, account.account_id)
        return AccountEvent(account_id=event.account_id, event_id=event.event_id, tier=account.tier)


def test_latest_by_renders_spark_visible_row_number_window() -> None:
    plan = _recipe(LatestEventTransform)

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert (
        'events = events.withColumn("__structure_latest_events_latest_rank", '
        'F.row_number().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").desc())))'
    ) in text
    assert 'events = events.where(F.col("__structure_latest_events_latest_rank") == F.lit(1))' in text
    assert 'events = events.drop("__structure_latest_events_latest_rank")' in text


def test_dedupe_latest_by_renders_deterministic_selected_row_window() -> None:
    plan = _recipe(LatestDedupeEventTransform)

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert (
        'events = events.withColumn("__structure_latest_events_latest_rank", '
        'F.row_number().over(Window.partitionBy(F.col("raw_event.account_id")).'
        'orderBy(F.col("raw_event.sequence").desc())))'
    ) in text
    assert 'events = events.where(F.col("__structure_latest_events_latest_rank") == F.lit(1))' in text


def test_earliest_by_records_one_ascending_selected_row_operation() -> None:
    plan = _recipe(EarliestEventTransform)

    assert len(plan.steps[0].operations) == 1
    operation = plan.steps[0].operations[0]
    assert operation.selected_rows is not None
    assert operation.selected_rows.direction == "earliest"


def test_dedupe_earliest_by_records_selected_row_operation() -> None:
    plan = _recipe(EarliestDedupeEventTransform)
    operation = plan.steps[0].operations[0]

    assert operation.selected_rows is not None
    assert operation.selected_rows.direction == "earliest"
    assert operation.selected_rows.partition_by[0].data is not None
    assert operation.selected_rows.partition_by[0].data["field"] == "account_id"


def test_window_projection_helpers_render_spark_visible_windows() -> None:
    plan = _recipe(RankedEventTransform)

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
    plan = _recipe(AdvancedRankedEventTransform)

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
    plan = _recipe(MultiOrderedEventTransform)

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    order = 'orderBy(F.col("raw_event.sequence").asc_nulls_last(), F.col("raw_event.event_id").desc_nulls_first())'
    assert f"F.rank().over(Window.partitionBy(F.col(\"raw_event.account_id\")).{order})" in text
    assert (
        f"F.sum(F.col(\"raw_event.sequence\")).over(Window.partitionBy(F.col(\"raw_event.account_id\")).{order}.rowsBetween(-2, Window.currentRow))"
        in text
    )


def test_window_requires_at_least_one_order_key() -> None:
    with pytest.raises(TypeError, match="window\\(\\.\\.\\.\\) requires at least one order_by expression"):
        window(partition_by=RawEvent.account_id, order_by=())


def test_window_aggregate_helpers_render_over_an_explicit_frame() -> None:
    plan = _recipe(AggregateWindowEventTransform)

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
    spec = window(partition_by="account", order_by="sequence")

    with pytest.raises(TypeError, match="window_bool_and\\(\\.\\.\\.\\) requires a Boolean expression"):
        window_bool_and(RawEvent.sequence, over=spec)
    with pytest.raises(TypeError, match="window_stddev\\(\\.\\.\\.\\) requires a numeric expression"):
        window_stddev(RawEvent.event_id, over=spec)
    with pytest.raises(TypeError, match="does not permit distinct window aggregates"):
        window_count_distinct(RawEvent.sequence, over=spec)
    with pytest.raises(
        TypeError, match="bounded range_between\\(\\.\\.\\.\\) requires exactly one order_by expression"
    ):
        window(
            partition_by="account",
            order_by=("sequence", "event_id"),
            frame=range_between(preceding(1), current_row()),
        )
    with pytest.raises(TypeError, match="bounded range_between\\(\\.\\.\\.\\) requires a numeric order_by expression"):
        window(
            partition_by="account",
            order_by="sequence",
            frame=range_between(current_row(), current_row()),
        )
    assert window(
        partition_by="account",
        order_by=("sequence", "event_id"),
        frame=range_between(unbounded_preceding(), unbounded_following()),
    )


def test_window_projection_helpers_add_window_import_to_generated_module() -> None:
    plan = _recipe(RankedEventTransform)

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
    plan = _recipe(UniqueEventTransform)

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert "events = events.dropDuplicates()" in text


def test_distinct_relation_renders_exact_relation_duplicate_removal() -> None:
    plan = _recipe(UniqueRelationEventTransform)

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert 'events = events.dropDuplicates(["account_id", "event_id", "sequence"])' in text


def test_drop_duplicates_explain_names_dedupe_operation_and_streaming_status() -> None:
    text = render_explain_report(UniqueEventTransform)

    assert "operations: drop_duplicates(row_filtering streaming_modes=append)" in text
    assert "STREAM-E0801: batch_only in unique_events (exact duplicate removal)" in text


def test_drop_duplicates_renders_subset_columns_when_requested() -> None:
    plan = _recipe(UniqueAccountEventTransform)

    text = render_pyspark_step(plan.steps[0], current="events", sources={"events": "events"})

    assert 'events = events.dropDuplicates(["account_id"])' in text


def test_drop_duplicates_subset_explain_names_subset_and_streaming_status() -> None:
    text = render_explain_report(UniqueAccountEventTransform)

    assert "operations: drop_duplicates(row_filtering subset=1 scope=events streaming_modes=append)" in text
    assert "STREAM-E0801: batch_only in unique_events (subset duplicate removal)" in text


def test_relation_drop_duplicates_before_join_prepares_join_source() -> None:
    plan = _recipe(PreJoinUniqueAccountTransform)

    text = render_pyspark_step(
        plan.steps[0],
        current="events",
        sources={"events": "events", "accounts": "accounts"},
    )

    assert 'events_account_deduped_1 = accounts.dropDuplicates(["account_id"])' in text
    assert text.index("events_account_deduped_1 =") < text.index("events = events.join(")


def test_relation_drop_duplicates_after_join_applies_to_joined_frame() -> None:
    plan = _recipe(PostJoinUniqueAccountTransform)

    text = render_pyspark_step(
        plan.steps[0],
        current="events",
        sources={"events": "events", "accounts": "accounts"},
    )

    assert 'events = events.dropDuplicates(["account_id"])' in text
    assert text.index("events = events.join(") < text.index('events = events.dropDuplicates(["account_id"])')


def test_drop_duplicates_rejects_mixed_relation_scopes() -> None:
    with pytest.raises(TypeError, match="one relation scope"):
        _compile(MixedScopeDropDuplicatesTransform)
