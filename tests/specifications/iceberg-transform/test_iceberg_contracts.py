"""Spark-free contracts for caller-bound Iceberg table helpers."""

from datetime import datetime, timezone

import pytest

from structure import Schema, Transform, input, output, step, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    iceberg_append,
    iceberg_delete,
    iceberg_expire_snapshots,
    iceberg_history,
    iceberg_input,
    iceberg_merge,
    iceberg_metadata,
    iceberg_output,
    iceberg_remove_orphan_files,
    iceberg_rewrite_data_files,
    iceberg_rewrite_manifests,
    iceberg_rollback,
    iceberg_snapshot,
    iceberg_snapshots,
    iceberg_table,
    iceberg_update,
    integer,
    long,
    string,
)
from structure.plugin.pyspark.iceberg.runtime import quote_table_name


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


class Change(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


class OrderV2(Order):
    note = string()


class SnapshotHistory(Schema):
    snapshot_id = long(nullable=False)


def _compile(subject, **plugin):
    return Compiler.frontend.compile()(subject, materialize_schemas=False, plugin={"pyspark": plugin})


def test_iceberg_table_is_caller_bound_and_retains_delete_effect():
    @transform
    class Delete(Transform):
        orders = iceberg_table(Order)

        def delete(self, order: Order) -> None:
            iceberg_delete(order, where=order.id == 1)

    name = "warehouse.sales.orders"
    assert Delete(orders=name)._structure_bound_inputs["orders"] == name
    plan = _compile(Delete).lowered
    assert plan.inputs[0].binding == "iceberg_table"
    assert plan.steps[0].effect
    assert plan.steps[0].delta_mutations[0].kind == "iceberg_delete"
    assert plan.outputs[0].binding == "iceberg_table"

    import ast

    from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module

    generated = render_pyspark_transform_module(
        plan, source_transform=f"{__name__}.Delete", schema_modules={Order: __name__}, runtime_module="tests.runtime"
    )
    ast.parse(generated)
    assert "self._iceberg_tables['orders']" in generated
    assert "DELETE FROM" in generated
    assert "structure.plugin.pyspark.delta.runtime" not in generated


def test_iceberg_merge_retains_a_same_schema_table_result():
    @transform
    class Merge(Transform):
        changes = input(Change)
        orders = iceberg_table(Order)

        def merge(self, change: Change, order: Order) -> Order:
            return (
                iceberg_merge(order, change, on=order.id == change.id)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

    step = _compile(Merge).lowered.steps[0]
    assert step.effect
    assert step.delta_mutations[0].kind == "iceberg_merge"
    assert step.delta_mutations[0].output_schema is Order


def test_iceberg_append_evolution_resolves_declared_output_schema():
    @transform
    class Evolve(Transform):
        changes = input(OrderV2)
        orders = iceberg_input(Order)
        evolved = iceberg_output(OrderV2)

        @step(input=(changes, orders), output=evolved)
        def append(self, change: OrderV2, order: Order) -> OrderV2:
            return iceberg_append(order, change).with_schema_evolution(to=OrderV2).execute()

    mutation = _compile(Evolve).lowered.steps[0].delta_mutations[0]
    assert mutation.kind == "iceberg_append"
    assert mutation.schema_evolution
    assert mutation.output_schema is OrderV2
    assert mutation.output == "evolved"


def test_iceberg_input_is_read_only_except_for_evolving_append():
    @transform
    class Invalid(Transform):
        orders = iceberg_input(Order)
        result = output(Order)

        def delete(self, order: Order) -> Order:
            iceberg_delete(order, where=True)
            return order

    with pytest.raises(TypeError, match="read-only"):
        _compile(Invalid)


def test_iceberg_identifier_parser_quotes_each_component_safely():
    assert quote_table_name("warehouse.sales.orders") == "`warehouse`.`sales`.`orders`"
    assert quote_table_name("`lake.house`.sales.`order``line`") == "`lake.house`.`sales`.`order``line`"
    with pytest.raises(ValueError, match="explicit catalog"):
        quote_table_name("orders")
    with pytest.raises(ValueError, match="Invalid"):
        quote_table_name("warehouse..orders")


def test_iceberg_capability_admission_is_variant_and_profile_specific():
    from structure.plugin.api.v1.model import CapabilityRequirement
    from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities

    requirement = CapabilityRequirement(group="iceberg", name="binding")
    assert PySparkCapabilities(target_profile=">=3.5,<4.1").supports(requirement).supported
    for profile, variant in ((">=4.2,<4.3", "ordinary"), (">=3.5,<4.0", "spark-connect")):
        assert not PySparkCapabilities(target_profile=profile, target_variant=variant).supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.0,<4.1").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.1,<4.2").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.1,<4.2", target_variant="spark-connect").supports(requirement).supported


def test_iceberg_history_is_a_typed_metadata_read():
    @transform
    class ReadHistory(Transform):
        orders = iceberg_table(Order)
        history = output(SnapshotHistory)

        @step(input=orders, output=history)
        def read(self, order: Order) -> SnapshotHistory:
            return iceberg_history(order, limit=5)

    plan = _compile(ReadHistory).lowered
    assert not plan.steps[0].effect
    assert plan.steps[0].delta_mutations[0].kind == "iceberg_history"
    assert plan.steps[0].delta_mutations[0].output_schema is SnapshotHistory


def test_iceberg_snapshot_and_metadata_helpers_capture_typed_reads():
    @transform
    class ReadSnapshot(Transform):
        orders = iceberg_table(Order)
        rows = output(Order)

        @step(input=orders, output=rows)
        def snapshot(self, order: Order) -> Order:
            return iceberg_snapshot(order, snapshot_id=42)

    @transform
    class ReadMetadata(Transform):
        orders = iceberg_table(Order)
        snapshots = output(SnapshotHistory)

        @step(input=orders, output=snapshots)
        def metadata(self, order: Order) -> SnapshotHistory:
            return iceberg_metadata(order, kind="snapshots", to=SnapshotHistory)

    @transform
    class ReadSnapshots(Transform):
        orders = iceberg_table(Order)
        snapshots = output(SnapshotHistory)

        @step(input=orders, output=snapshots)
        def snapshots_read(self, order: Order) -> SnapshotHistory:
            return iceberg_snapshots(order, limit=1)

    snapshot_plan = _compile(ReadSnapshot).lowered.steps[0]
    metadata_plan = _compile(ReadMetadata).lowered.steps[0]
    snapshots_plan = _compile(ReadSnapshots).lowered.steps[0]
    assert snapshot_plan.delta_mutations[0].kind == "iceberg_snapshot"
    assert snapshot_plan.delta_mutations[0].selector.data["value"] == 42
    assert metadata_plan.delta_mutations[0].kind == "iceberg_metadata"
    assert metadata_plan.delta_mutations[0].action == "snapshots"
    assert metadata_plan.delta_mutations[0].output_schema is SnapshotHistory
    assert snapshots_plan.delta_mutations[0].kind == "iceberg_snapshots"


def test_iceberg_native_maintenance_builders_capture_arguments():
    @transform
    class Maintain(Transform):
        orders = iceberg_table(Order)

        @step(input=orders, output=orders)
        def maintain(self, order: Order) -> None:
            iceberg_rollback(order, snapshot_id=42).execute()
            iceberg_rewrite_data_files(order, where=order.id > 0, options={"target-file-size-bytes": "1024"}).execute()
            iceberg_rewrite_manifests(order, use_caching=False, spec_id=0).execute()
            iceberg_expire_snapshots(
                order, older_than=datetime(2025, 1, 1, tzinfo=timezone.utc), retain_last=1
            ).execute()
            iceberg_remove_orphan_files(order, dry_run=True).execute()

    mutations = _compile(Maintain).lowered.steps[0].delta_mutations
    assert [mutation.action for mutation in mutations] == [
        "rollback_to_snapshot",
        "rewrite_data_files",
        "rewrite_manifests",
        "expire_snapshots",
        "remove_orphan_files",
    ]
    assert mutations[0].procedure_args == (("snapshot_id", 42),)
    assert dict(mutations[1].procedure_args)["options"] == {"target-file-size-bytes": "1024"}
