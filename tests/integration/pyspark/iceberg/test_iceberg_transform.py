"""Direct and generated Structure execution against disposable Iceberg tables."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session

from structure import Schema, Transform, input, output, step, transform
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
    long,
    string,
)

pytestmark = pytest.mark.integration


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


@transform
class BaseCleanup(Transform):
    orders = iceberg_table(Order)

    def clean(self, order: Order) -> None:
        iceberg_delete(order, where=order.status == "legacy")


@transform
class ReplaceCleanup(BaseCleanup):
    def clean(self, order: Order) -> None:
        iceberg_delete(order, where=order.status == "archived")


@transform
class ExtendCleanup(BaseCleanup):
    def clean(self, order: Order) -> None:
        super().clean(order)
        iceberg_delete(order, where=order.status == "archived")


@transform
class ReadCleanedOrders(Transform):
    orders = iceberg_input(Order)
    selected = output(Order)

    def select(self, order: Order) -> Order:
        return Order.project(order)


class CleanupThenRead(Transform):
    orders = iceberg_table(Order)
    pipeline = BaseCleanup(orders=orders).to(ReadCleanedOrders())


class Change(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class OrderV1(Schema):
    id = string(nullable=False)


class OrderV2(OrderV1):
    note = string()


class SnapshotHistory(Schema):
    snapshot_id = long(nullable=False)


@transform
class ApplyIcebergChanges(Transform):
    changes = input(Change)
    orders = iceberg_table(Order)

    @step(input=(changes, orders), output=orders)
    def apply(self, change: Change, order: Order) -> None:
        iceberg_update(order, where=order.id == "A", set=Order(status="paid"))
        iceberg_delete(order, where=order.id == "B")
        iceberg_append(order, change).execute()


@transform
class MergeIcebergChanges(Transform):
    changes = input(Change)
    orders = iceberg_table(Order)

    def merge(self, change: Change, order: Order) -> Order:
        return (
            iceberg_merge(order, change, on=order.id == change.id)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


@transform
class EvolveIcebergAppend(Transform):
    changes = input(OrderV2)
    current_orders = iceberg_input(OrderV1)
    orders = iceberg_output(OrderV2)

    @step(input=(changes, current_orders), output=orders)
    def append(self, change: OrderV2, order: OrderV1) -> OrderV2:
        return iceberg_append(order, change).with_schema_evolution(to=OrderV2).execute()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_typed_iceberg_mutations_match_direct_and_generated_execution(spark, tmp_path, mode) -> None:
    name = f"structure_iceberg.default.transform_{uuid4().hex}"
    spark.sql(
        f"CREATE TABLE {name} (id STRING NOT NULL, status STRING NOT NULL) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {name} VALUES ('A', 'new'), ('B', 'new')")
    changes = spark.createDataFrame([("C", "new")], "id STRING, status STRING")
    files = render_generated_project(
        ApplyIcebergChanges,
        source_transform=f"{ApplyIcebergChanges.__module__}.{ApplyIcebergChanges.__name__}",
        generated_package="tests.generated_iceberg",
        source_schema_modules={Order.__module__: [Order, Change]},
    )
    try:
        with generated_project(tmp_path, "tests.generated_iceberg", files):
            result = ApplyIcebergChanges(changes=changes, orders=name).run(
                session(spark, execution_mode=mode, generated_package="tests.generated_iceberg")
            )
        assert result.orders == name
        assert {(row.id, row.status) for row in spark.table(name).collect()} == {
            ("A", "paid"),
            ("C", "new"),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_iceberg_history_read_and_rewrite_helper(spark, tmp_path, mode) -> None:
    name = f"structure_iceberg.default.inspect_{uuid4().hex}"
    spark.sql(f"CREATE TABLE {name} (id STRING NOT NULL, status STRING) USING iceberg")
    spark.sql(f"INSERT INTO {name} VALUES ('A', 'new')")

    @transform
    class InspectIceberg(Transform):
        orders = iceberg_table(Order)
        history_rows = output(SnapshotHistory)

        @step(input=orders, output=history_rows)
        def history(self, order: Order) -> SnapshotHistory:
            return iceberg_history(order, limit=2)

    @transform
    class RewriteIceberg(Transform):
        orders = iceberg_table(Order)

        def rewrite(self, order: Order) -> None:
            iceberg_rewrite_data_files(order).execute()

    inspect_files = render_generated_project(
        InspectIceberg,
        source_transform=f"{InspectIceberg.__module__}.{InspectIceberg.__name__}",
        generated_package="tests.generated_iceberg_inspect",
        source_schema_modules={Order.__module__: [Order, SnapshotHistory]},
    )
    rewrite_files = render_generated_project(
        RewriteIceberg,
        source_transform=f"{RewriteIceberg.__module__}.{RewriteIceberg.__name__}",
        generated_package="tests.generated_iceberg_rewrite",
        source_schema_modules={Order.__module__: [Order]},
    )
    try:
        with generated_project(tmp_path, "tests.generated_iceberg_inspect", inspect_files):
            history = InspectIceberg(orders=name).run(
                session(spark, execution_mode=mode, generated_package="tests.generated_iceberg_inspect")
            ).history_rows
            assert len(history.collect()) == 1
        with generated_project(tmp_path, "tests.generated_iceberg_rewrite", rewrite_files):
            result = RewriteIceberg(orders=name).run(
                session(spark, execution_mode=mode, generated_package="tests.generated_iceberg_rewrite")
            )
        assert result.orders == name
        assert spark.table(name).count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_iceberg_snapshot_and_metadata_reads(spark, tmp_path, mode) -> None:
    name = f"structure_iceberg.default.snapshots_{uuid4().hex}"
    spark.sql(f"CREATE TABLE {name} (id STRING NOT NULL, status STRING NOT NULL) USING iceberg")
    spark.sql(f"INSERT INTO {name} VALUES ('A', 'old')")
    snapshot_id = spark.sql(f"SELECT snapshot_id FROM {name}.snapshots").first()[0]
    spark.sql(f"INSERT INTO {name} VALUES ('B', 'new')")

    @transform
    class InspectSnapshot(Transform):
        orders = iceberg_table(Order)
        prior = output(Order)
        snapshots = output(SnapshotHistory)
        metadata = output(SnapshotHistory)

        @step(input=orders, output=prior)
        def prior_snapshot(self, order: Order) -> Order:
            return iceberg_snapshot(order, snapshot_id=snapshot_id)

        @step(input=orders, output=snapshots)
        def recent_snapshots(self, order: Order) -> SnapshotHistory:
            return iceberg_snapshots(order, limit=2)

        @step(input=orders, output=metadata)
        def snapshot_metadata(self, order: Order) -> SnapshotHistory:
            return iceberg_metadata(order, kind="snapshots", to=SnapshotHistory)

    files = render_generated_project(
        InspectSnapshot,
        source_transform=f"{InspectSnapshot.__module__}.{InspectSnapshot.__name__}",
        generated_package="tests.generated_iceberg_snapshot",
        source_schema_modules={Order.__module__: [Order, SnapshotHistory]},
    )
    try:
        with generated_project(tmp_path, "tests.generated_iceberg_snapshot", files):
            result = InspectSnapshot(orders=name).run(
                session(spark, execution_mode=mode, generated_package="tests.generated_iceberg_snapshot")
            )
        assert [(row.id, row.status) for row in result.prior.collect()] == [("A", "old")]
        assert len(result.snapshots.collect()) == 2
        assert len(result.metadata.collect()) == 2
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_iceberg_rollback_and_retention_procedures(spark, tmp_path, mode) -> None:
    name = f"structure_iceberg.default.retention_{uuid4().hex}"
    spark.sql(f"CREATE TABLE {name} (id STRING NOT NULL, status STRING NOT NULL) USING iceberg")
    spark.sql(f"INSERT INTO {name} VALUES ('A', 'old')")
    snapshot_id = spark.sql(f"SELECT snapshot_id FROM {name}.snapshots").first()[0]
    spark.sql(f"INSERT INTO {name} VALUES ('B', 'new')")

    @transform
    class MaintainIceberg(Transform):
        orders = iceberg_table(Order)

        def maintain(self, order: Order) -> None:
            iceberg_rollback(order, snapshot_id=snapshot_id).execute()
            iceberg_rewrite_manifests(order).execute()
            iceberg_expire_snapshots(
                order, older_than=datetime(2025, 1, 1, tzinfo=timezone.utc), retain_last=1
            ).execute()
            iceberg_remove_orphan_files(order, dry_run=True).execute()

    files = render_generated_project(
        MaintainIceberg,
        source_transform=f"{MaintainIceberg.__module__}.{MaintainIceberg.__name__}",
        generated_package="tests.generated_iceberg_retention",
        source_schema_modules={Order.__module__: [Order]},
    )
    try:
        with generated_project(tmp_path, "tests.generated_iceberg_retention", files):
            result = MaintainIceberg(orders=name).run(
                session(spark, execution_mode=mode, generated_package="tests.generated_iceberg_retention")
            )
        assert result.orders == name
        assert {(row.id, row.status) for row in spark.table(name).collect()} == {("A", "old")}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_iceberg_merge_preserves_native_rows(spark) -> None:
    name = f"structure_iceberg.default.merge_{uuid4().hex}"
    spark.sql(
        f"CREATE TABLE {name} (id STRING NOT NULL, status STRING NOT NULL) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {name} VALUES ('A', 'new')")
    changes = spark.createDataFrame([("A", "paid"), ("B", "new")], "id STRING, status STRING")
    try:
        result = MergeIcebergChanges(changes=changes, orders=name).run(
            session(spark, execution_mode="online")
        )
        assert result.orders == name
        assert {(row.id, row.status) for row in spark.table(name).collect()} == {
            ("A", "paid"),
            ("B", "new"),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_iceberg_append_evolution_requires_property_and_returns_new_schema(spark, tmp_path, mode) -> None:
    name = f"structure_iceberg.default.evolving_transform_{uuid4().hex}"
    spark.sql(
        f"CREATE TABLE {name} (id STRING NOT NULL) USING iceberg "
        "TBLPROPERTIES ('format-version'='2', 'write.spark.accept-any-schema'='true')"
    )
    changes = spark.createDataFrame([("A", "first"), ("B", "second")], "id STRING, note STRING")
    files = render_generated_project(
        EvolveIcebergAppend,
        source_transform=f"{EvolveIcebergAppend.__module__}.{EvolveIcebergAppend.__name__}",
        generated_package="tests.generated_iceberg_evolution",
        source_schema_modules={OrderV1.__module__: [OrderV1, OrderV2]},
    )
    try:
        with generated_project(tmp_path, "tests.generated_iceberg_evolution", files):
            result = EvolveIcebergAppend(changes=changes, current_orders=name).run(
                session(spark, execution_mode=mode, generated_package="tests.generated_iceberg_evolution")
            )
        assert result.orders == name
        assert spark.table(name).columns == ["id", "note"]
        assert {(row.id, row.note) for row in spark.table(name).collect()} == {
            ("A", "first"),
            ("B", "second"),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize(
    ("subject", "expected"),
    [
        (ReplaceCleanup, {("A", "open"), ("B", "legacy")}),
        (ExtendCleanup, {("A", "open")}),
    ],
)
def test_table_inheritance_overrides_and_super_match_live_rows(spark, tmp_path, mode, subject, expected) -> None:
    name = f"structure_iceberg.default.inherit_{uuid4().hex}"
    spark.sql(f"CREATE TABLE {name} (id STRING NOT NULL, status STRING NOT NULL) USING iceberg")
    spark.sql(f"INSERT INTO {name} VALUES ('A', 'open'), ('B', 'legacy'), ('C', 'archived')")
    package = f"tests.generated_iceberg_{subject.__name__.lower()}"
    files = render_generated_project(
        subject,
        source_transform=f"{subject.__module__}.{subject.__name__}",
        generated_package=package,
        source_schema_modules={Order.__module__: [Order]},
    )
    try:
        with generated_project(tmp_path, package, files):
            result = subject(orders=name).run(
                session(spark, execution_mode=mode, generated_package=package)
            )
        assert result.orders == name
        assert {(row.id, row.status) for row in spark.table(name).collect()} == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_composed_iceberg_stages_read_after_the_parent_commit(spark, tmp_path, mode) -> None:
    name = f"structure_iceberg.default.compose_{uuid4().hex}"
    spark.sql(f"CREATE TABLE {name} (id STRING NOT NULL, status STRING NOT NULL) USING iceberg")
    spark.sql(f"INSERT INTO {name} VALUES ('A', 'open'), ('B', 'legacy'), ('C', 'archived')")
    package = f"tests.generated_iceberg_compose_{mode}"
    files = render_generated_project(
        CleanupThenRead,
        source_transform=f"{CleanupThenRead.__module__}.{CleanupThenRead.__name__}",
        generated_package=package,
        source_schema_modules={Order.__module__: [Order]},
    )
    try:
        with generated_project(tmp_path, package, files):
            result = CleanupThenRead(orders=name).run(
                session(spark, execution_mode=mode, generated_package=package)
            )
        assert {(row.id, row.status) for row in result.selected.collect()} == {
            ("A", "open"),
            ("C", "archived"),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
