"""Disposable native Delta table evidence for transform-owned mutations."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest
from integration.pyspark.support.backend_matrix import (
    generated_project,
    render_generated_project,
    render_generated_projects,
    session,
)

from structure import Schema, Transform, input, output, step, transform, variable
from structure.plugin.pyspark import (
    array,
    check,
    delta_append,
    delta_changes,
    delta_default,
    delta_delete,
    delta_detail,
    delta_generated,
    delta_history,
    delta_identity,
    delta_input,
    delta_merge,
    delta_optimize,
    delta_output,
    delta_replace_where,
    delta_restore,
    delta_snapshot,
    delta_table,
    delta_update,
    delta_vacuum,
    long,
    string,
    timestamp,
)

pytestmark = pytest.mark.integration
PACKAGE = "integration_v11_delta_transform_generated"


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)
    constraints = (check(status != "invalid", name="valid_status"),)


@transform
class BaseCleanup(Transform):
    orders = delta_table(Order)

    def clean(self, order: Order) -> None:
        delta_delete(order, where=order.status == "legacy")


@transform
class ReplaceCleanup(BaseCleanup):
    def clean(self, order: Order) -> None:
        delta_delete(order, where=order.status == "archived")


@transform
class ExtendCleanup(BaseCleanup):
    def clean(self, order: Order) -> None:
        super().clean(order)
        delta_delete(order, where=order.status == "archived")


@transform
class ReadCleanedOrders(Transform):
    orders = delta_input(Order)
    selected = output(Order)

    def select(self, order: Order) -> Order:
        return Order.project(order)


class CleanupThenRead(Transform):
    orders = delta_table(Order)
    pipeline = BaseCleanup(orders=orders).to(ReadCleanedOrders())


class Change(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class ChangeV2(Change):
    note = string()


class OrderV2(Order):
    note = string()


class OrderChange(Schema):
    id = string()
    status = string()
    change_type = string(alias="_change_type")
    commit_version = long(alias="_commit_version")
    commit_timestamp = timestamp(alias="_commit_timestamp")


class OrderCommit(Schema):
    version = long()
    operation = string()


class OrderDetail(Schema):
    format = string()
    location = string()


class LiquidOrderDetail(OrderDetail):
    clustering_columns = array(string(), contains_null=False, alias="clusteringColumns")


class LayoutOrder(Schema):
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    order_date = string(nullable=False)


class GeneratedColumnSchema(Schema):
    base = long(nullable=False)
    derived = long()
    delta_columns = (delta_generated(derived, as_="base + 1"),)


class WrongGeneratedColumnSchema(Schema):
    base = long(nullable=False)
    derived = long()
    delta_columns = (delta_generated(derived, as_="base + 2"),)


class IdentityColumnSchema(Schema):
    id = long()
    value = string(nullable=False)
    delta_columns = (delta_identity(id),)


class WrongIdentityColumnSchema(Schema):
    id = long()
    value = string(nullable=False)
    delta_columns = (delta_identity(id, start=100),)


class DefaultColumnSchema(Schema):
    id = long(nullable=False)
    value = string()
    delta_columns = (delta_default(value, value="open"),)


class WrongDefaultColumnSchema(Schema):
    id = long(nullable=False)
    value = string()
    delta_columns = (delta_default(value, value="closed"),)


class GeneratedSource(Schema):
    base = long(nullable=False)


class IdentitySource(Schema):
    value = string(nullable=False)


class DefaultSource(Schema):
    id = long(nullable=False)


@transform
class AppendGeneratedColumn(Transform):
    rows = input(GeneratedSource)
    orders = delta_table(GeneratedColumnSchema)

    @step(input=(rows, orders), output=orders)
    def append(self, row: GeneratedSource, order: GeneratedColumnSchema) -> None:
        delta_append(order, row).execute()


@transform
class AppendWrongGeneratedColumn(Transform):
    rows = input(GeneratedSource)
    orders = delta_table(WrongGeneratedColumnSchema)

    @step(input=(rows, orders), output=orders)
    def append(self, row: GeneratedSource, order: WrongGeneratedColumnSchema) -> None:
        delta_append(order, row).execute()


@transform
class InsertIdentityColumn(Transform):
    rows = input(IdentitySource)
    orders = delta_table(IdentityColumnSchema)

    @step(input=(rows, orders), output=orders)
    def insert(self, row: IdentitySource, order: IdentityColumnSchema) -> None:
        (
            delta_merge(order, row, on=order.value == row.value)
            .when_not_matched_insert(values=IdentityColumnSchema(value=row.value))
            .execute()
        )


@transform
class InsertWrongIdentityColumn(Transform):
    rows = input(IdentitySource)
    orders = delta_table(WrongIdentityColumnSchema)

    @step(input=(rows, orders), output=orders)
    def insert(self, row: IdentitySource, order: WrongIdentityColumnSchema) -> None:
        (
            delta_merge(order, row, on=order.value == row.value)
            .when_not_matched_insert(values=WrongIdentityColumnSchema(value=row.value))
            .execute()
        )


@transform
class InsertDefaultColumn(Transform):
    rows = input(DefaultSource)
    orders = delta_table(DefaultColumnSchema)

    @step(input=(rows, orders), output=orders)
    def insert(self, row: DefaultSource, order: DefaultColumnSchema) -> None:
        (
            delta_merge(order, row, on=order.id == row.id)
            .when_not_matched_insert(values=DefaultColumnSchema(id=row.id))
            .execute()
        )


@transform
class InsertWrongDefaultColumn(Transform):
    rows = input(DefaultSource)
    orders = delta_table(WrongDefaultColumnSchema)

    @step(input=(rows, orders), output=orders)
    def insert(self, row: DefaultSource, order: WrongDefaultColumnSchema) -> None:
        (
            delta_merge(order, row, on=order.id == row.id)
            .when_not_matched_insert(values=WrongDefaultColumnSchema(id=row.id))
            .execute()
        )


@transform
class ReadOrderMetadata(Transform):
    orders = delta_input(Order)
    limit = variable(int, default=2)
    commits = output(OrderCommit)
    details = output(OrderDetail)

    @step(input=orders, output=commits)
    def history(self, order: Order) -> OrderCommit:
        return delta_history(order, limit=self.limit)

    @step(input=orders, output=details)
    def detail(self, order: Order) -> OrderDetail:
        return delta_detail(order)


@transform
class ReadLiquidOrderMetadata(Transform):
    orders = delta_input(LayoutOrder)
    details = output(LiquidOrderDetail)

    @step(input=orders, output=details)
    def detail(self, order: LayoutOrder) -> LiquidOrderDetail:
        return delta_detail(order)


@transform
class RestoreOrders(Transform):
    orders = delta_table(Order)
    version = variable(int)

    def restore(self, order: Order) -> Order:
        return delta_restore(order, version=self.version).execute()


@transform
class RestoreSchemaOrders(Transform):
    current_orders = delta_input(OrderV2)
    orders = delta_output(Order)
    version = variable(int)

    def restore(self, order: OrderV2) -> Order:
        return delta_restore(order, version=self.version).execute()


@transform
class CompactOrders(Transform):
    orders = delta_table(Order)

    @step(inout=orders | orders)
    def compact(self, order: Order) -> None:
        delta_optimize(order).execute_compaction()


@transform
class ZOrderOrders(Transform):
    orders = delta_table(Order)

    @step(inout=orders | orders)
    def optimize(self, order: Order) -> None:
        delta_optimize(order).execute_zorder(by=(order.id,))


class ZOrderLayout(Transform):
    orders = delta_table(LayoutOrder)

    def optimize(self, order: LayoutOrder) -> None:
        delta_optimize(order).execute_zorder(by=(order.customer_id, order.product_id))


class ZOrderPartition(Transform):
    orders = delta_table(LayoutOrder)
    selected_date = variable(str)

    def optimize(self, order: LayoutOrder) -> None:
        delta_optimize(order, where=order.order_date == self.selected_date).execute_zorder(
            by=(order.customer_id, order.product_id),
        )


@transform
class IncrementalClusteredOrders(Transform):
    orders = delta_table(LayoutOrder)

    @step(inout=orders | orders)
    def optimize(self, order: LayoutOrder) -> None:
        delta_optimize(order).execute_compaction()


@transform
class FullReclusterOrders(Transform):
    orders = delta_table(LayoutOrder)

    @step(inout=orders | orders)
    def recluster(self, order: LayoutOrder) -> None:
        delta_optimize(order).full()


@transform
class OptimizeClusteredWhere(Transform):
    orders = delta_table(LayoutOrder)

    @step(inout=orders | orders)
    def optimize(self, order: LayoutOrder) -> None:
        delta_optimize(order, where=order.order_date == "2026-10-08").execute_compaction()


@transform
class ZOrderClusteredOrders(Transform):
    orders = delta_table(LayoutOrder)

    @step(inout=orders | orders)
    def optimize(self, order: LayoutOrder) -> None:
        delta_optimize(order).execute_zorder(by=(order.customer_id,))


@transform
class AppendLiquidOrders(Transform):
    rows = input(LayoutOrder)
    orders = delta_table(LayoutOrder)

    @step(input=(rows, orders), output=orders)
    def append(self, row: LayoutOrder, order: LayoutOrder) -> None:
        delta_append(order, row).execute()


@transform
class UpdateLiquidOrders(Transform):
    orders = delta_table(LayoutOrder)

    @step(inout=orders | orders)
    def update(self, order: LayoutOrder) -> None:
        delta_update(order, where=order.customer_id == "c0", set=LayoutOrder(product_id="updated"))


@transform
class MergeLiquidOrders(Transform):
    changes = input(LayoutOrder)
    orders = delta_table(LayoutOrder)

    @step(input=(changes, orders), output=orders)
    def merge(self, change: LayoutOrder, order: LayoutOrder) -> None:
        (
            delta_merge(
                order,
                change,
                on=(order.customer_id == change.customer_id) & (order.order_date == change.order_date),
            )
            .when_matched_update(set=LayoutOrder(product_id=change.product_id))
            .when_not_matched_insert_all()
            .execute()
        )


@transform
class DeleteLiquidOrders(Transform):
    orders = delta_table(LayoutOrder)

    @step(inout=orders | orders)
    def delete(self, order: LayoutOrder) -> None:
        delta_delete(order, where=order.customer_id == "c0")


@transform
class ReadLiquidOrders(Transform):
    orders = delta_input(LayoutOrder)
    rows = output(LayoutOrder)

    @step(input=orders, output=rows)
    def read(self, order: LayoutOrder) -> LayoutOrder:
        return LayoutOrder.project(order)


class ZOrderInvalidPartition(Transform):
    orders = delta_table(LayoutOrder)

    def optimize(self, order: LayoutOrder) -> None:
        delta_optimize(order, where=order.customer_id == "c1").execute_zorder(
            by=(order.customer_id, order.product_id),
        )


@transform
class VacuumOrders(Transform):
    orders = delta_table(Order)
    retention = variable(float, default=168.0)

    @step(inout=orders | orders)
    def vacuum(self, order: Order) -> None:
        delta_vacuum(order, retention_hours=self.retention, allow_short_retention=True).execute()


@transform
class Apply(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    @step(input=(changes, orders), output=orders)
    def apply(self, change: Change, order: Order) -> None:
        delta_delete(order, where=order.id == "2")
        delta_update(order, where=order.id == "1", set=Order(status="pending"))
        (
            delta_merge(order, change, on=order.id == change.id)
            .when_matched_update(set=Order(status=change.status))
            .when_not_matched_insert(values=Order(id=change.id, status=change.status))
            .execute()
        )


@transform
class MergeFamilies(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    @step(input=(changes, orders), output=orders)
    def merge(self, change: Change, order: Order) -> None:
        (
            delta_merge(order, change, on=order.id == change.id)
            .when_matched_delete(condition=change.status == "delete")
            .when_matched_update(set=Order(status=change.status), condition=change.status == "update")
            .when_matched_update_all()
            .when_not_matched_insert(
                values=Order(id=change.id, status=change.status), condition=change.status == "insert"
            )
            .when_not_matched_insert_all()
            .execute()
        )


@transform
class MergeBySource(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    @step(input=(changes, orders), output=orders)
    def merge(self, change: Change, order: Order) -> None:
        (
            delta_merge(order, change, on=order.id == change.id)
            .when_not_matched_by_source_update(set=Order(status="stale"), condition=order.status == "keep")
            .when_not_matched_by_source_delete()
            .execute()
        )


@transform(delta_check_match="name")
class NameOnly(Transform):
    orders = delta_table(Order)

    @step(input=orders, output=orders)
    def delete(self, order: Order) -> None:
        delta_delete(order, where=order.id == "2")


@transform(delta_check_match="off")
class ChecksOff(Transform):
    orders = delta_table(Order)

    @step(input=orders, output=orders)
    def delete(self, order: Order) -> None:
        delta_delete(order, where=order.id == "2")


@transform
class InvalidUpdate(Transform):
    orders = delta_table(Order)

    @step(input=orders, output=orders)
    def update(self, order: Order) -> None:
        delta_update(order, where=order.id == "1", set=Order(status="invalid"))


@transform
class ReadOrders(Transform):
    orders = delta_input(Order)
    viewed = output(Order)

    @step(input=orders, output=viewed)
    def view(self, order: Order) -> Order:
        return Order.project(order)


@transform
class ReadChanges(Transform):
    orders = delta_input(Order)
    starting_version = variable(int)
    ending_version = variable(int | None, default=None)
    changes = output(OrderChange)

    def read(self, order: Order) -> OrderChange:
        return delta_changes(
            order,
            starting_version=self.starting_version,
            ending_version=self.ending_version,
        )


@transform
class ReadSnapshot(Transform):
    orders = delta_input(Order)
    version = variable(int)
    snapshot = output(Order)

    def read(self, order: Order) -> Order:
        return delta_snapshot(order, version=self.version)


@transform
class ReplaceWest(Transform):
    replacements = input(Order)
    orders = delta_table(Order)

    @step(input=(orders, replacements), output=orders)
    def replace(self, order: Order, replacement: Order) -> None:
        delta_replace_where(order, replacement, where=order.status == "west").execute()


@transform
class EvolvingMerge(Transform):
    changes = input(ChangeV2)
    current_orders = delta_input(Order)
    orders = delta_output(OrderV2)

    def merge(self, change: ChangeV2, order: Order) -> OrderV2:
        return (
            delta_merge(order, change, on=order.id == change.id)
            .with_schema_evolution(to=OrderV2)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


@transform
class EvolvingAppend(Transform):
    changes = input(ChangeV2)
    current_orders = delta_input(Order)
    orders = delta_output(OrderV2)

    def append(self, change: ChangeV2, order: Order) -> OrderV2:
        return delta_append(order, change).with_schema_evolution(to=OrderV2).execute()


@transform
class EvolvingTableMerge(Transform):
    changes = input(ChangeV2)
    orders = delta_table(Order)

    def merge(self, change: ChangeV2, order: Order) -> None:
        (
            delta_merge(order, change, on=order.id == change.id)
            .with_schema_evolution(to=OrderV2)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


@pytest.fixture(scope="module")
def delta_spark(pytestconfig):
    pyspark = pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    backend = pytestconfig.getoption("--integration-backend")
    expected_versions = {
        "pyspark35": ("3.5.3", "3.3.3"),
        "pyspark40": ("4.0.0", "4.0.1"),
        "pyspark41": ("4.1.0", "4.1.0"),
        "spark-connect41": ("4.1.0", "4.1.0"),
    }
    expected = expected_versions.get(backend)
    if expected is None:
        pytest.skip("Delta transform evidence requires an admitted PySpark/Delta lane")
    from importlib.metadata import version

    actual = (pyspark.__version__, version("delta-spark"))
    if actual != expected:
        pytest.fail(f"Delta evidence for {backend} requires PySpark/Delta {expected}, got {actual}")
    if backend == "spark-connect41":
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.remote(os.environ["STRUCTURE_SPARK_REMOTE"]).getOrCreate()
        yield spark
        spark.stop()
        return
    from delta import configure_spark_with_delta_pip  # type: ignore[import-not-found]
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession() or getattr(SparkSession, "_instantiatedSession", None)
    if active is not None:
        active.stop()
    artifact_root = tempfile.mkdtemp(prefix="structure-delta-artifacts-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("structure-delta-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )
    working_directory = os.getcwd()
    try:
        # Spark 4.0 creates SQL artifacts relative to its working directory, but
        # the integration container mounts the repository read-only.
        os.chdir(artifact_root)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    finally:
        os.chdir(working_directory)
    yield spark
    spark.stop()
    shutil.rmtree(artifact_root)


def _table(
    spark, path: Path, *, native_check: bool, rows=(('1', 'open'), ('2', 'open')), check_sql="status <> 'invalid'"
):
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    spark.sql(f"CREATE TABLE delta.`{path}` (id STRING NOT NULL, status STRING NOT NULL) USING DELTA")
    values = ", ".join(f"('{id}', '{status}')" for id, status in rows)
    spark.sql(f"INSERT INTO delta.`{path}` VALUES {values}")
    if native_check:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT valid_status CHECK ({check_sql})")
    return DeltaTable.forPath(spark, str(path))


def _v1_table(spark, path: Path):
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    spark.sql(f"CREATE TABLE delta.`{path}` (id STRING NOT NULL, status STRING NOT NULL) USING DELTA")
    spark.sql(f"INSERT INTO delta.`{path}` VALUES ('1', 'open'), ('2', 'open')")
    spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT valid_status CHECK (status <> 'invalid')")
    return DeltaTable.forPath(spark, str(path))


def _enable_cdf(spark, path: Path):
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize(
    ("subject", "expected"),
    [
        (EvolvingMerge, [("1", "updated", "merged"), ("2", "open", None), ("3", "inserted", "new")]),
        (
            EvolvingAppend,
            [
                ("1", "open", None),
                ("1", "updated", "merged"),
                ("2", "open", None),
                ("3", "inserted", "new"),
            ],
        ),
    ],
)
def test_explicit_schema_evolution_is_scoped_and_verified(delta_spark, tmp_path, mode, subject, expected) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"evolution-{subject.__name__}-{mode}"
    table = _v1_table(delta_spark, path)
    changes = delta_spark.createDataFrame(
        [("1", "updated", "merged"), ("3", "inserted", "new")],
        ["id", "status", "note"],
    )
    files = render_generated_project(
        subject,
        source_transform=f"{subject.__module__}.{subject.__name__}",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, OrderV2, Change, ChangeV2]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        result = subject(changes=changes, current_orders=table).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    assert result.orders is table
    reopened = DeltaTable.forPath(delta_spark, str(path))
    assert sorted(tuple(row) for row in reopened.toDF().collect()) == expected


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_schema_evolution_can_target_delta_table_without_output(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"evolution-inout-{mode}"
    table = _v1_table(delta_spark, path)
    changes = delta_spark.createDataFrame(
        [("1", "updated", "merged"), ("3", "inserted", "new")],
        ["id", "status", "note"],
    )
    files = render_generated_project(
        EvolvingTableMerge,
        source_transform=f"{EvolvingTableMerge.__module__}.{EvolvingTableMerge.__name__}",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, OrderV2, Change, ChangeV2]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        result = EvolvingTableMerge(changes=changes, orders=table).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    assert result.orders is table
    reopened = DeltaTable.forPath(delta_spark, str(path))
    assert sorted(tuple(row) for row in reopened.toDF().collect()) == [
        ("1", "updated", "merged"),
        ("2", "open", None),
        ("3", "inserted", "new"),
    ]


def _changes(spark, rows=(("1", "changed"), ("3", "new"))):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("status", T.StringType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema)


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_cdf_variables_reuse_one_compiled_transform(delta_spark, tmp_path, mode) -> None:
    path = tmp_path / f"cdf-{mode}"
    table = _v1_table(delta_spark, path)
    _enable_cdf(delta_spark, path)
    delta_spark.sql(f"UPDATE delta.`{path}` SET status = 'first' WHERE id = '1'")
    first_version = table.history(1).first()["version"]
    delta_spark.sql(f"UPDATE delta.`{path}` SET status = 'second' WHERE id = '1'")
    second_version = table.history(1).first()["version"]

    run_session = session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
    first = ReadChanges(orders=table, starting_version=first_version)
    second = ReadChanges(orders=table, starting_version=second_version)
    assert run_session.compile(first).key == run_session.compile(second).key
    files = render_generated_projects(
        [
            (ReadChanges, f"{ReadChanges.__module__}.ReadChanges"),
            (ReadSnapshot, f"{ReadSnapshot.__module__}.ReadSnapshot"),
        ],
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, OrderChange]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        first_result = first.run(run_session).changes
        second_result = second.run(run_session).changes
        snapshot = ReadSnapshot(orders=table, version=first_version).run(run_session).snapshot
    first_postimages = [row.status for row in first_result.where("_change_type = 'update_postimage'").collect()]
    second_postimages = [row.status for row in second_result.where("_change_type = 'update_postimage'").collect()]
    assert sorted(first_postimages) == ["first", "second"]
    assert second_postimages == ["second"]
    assert {row.status for row in snapshot.collect()} == {"first", "open"}


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_delta_replace_where_commits_only_the_selected_slice(delta_spark, tmp_path, mode) -> None:
    path = tmp_path / f"replace-where-{mode}"
    table = _table(delta_spark, path, native_check=True, rows=(("1", "west"), ("2", "east")))
    replacements = _changes(delta_spark, rows=(("3", "west"),))
    files = render_generated_project(
        ReplaceWest,
        source_transform=f"{ReplaceWest.__module__}.ReplaceWest",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        result = ReplaceWest(orders=table, replacements=replacements).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    assert result.orders is table
    rows = sorted(tuple(row) for row in DeltaTable.forPath(delta_spark, str(path)).toDF().collect())
    assert rows == [("2", "east"), ("3", "west")]


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_native_mutations_preserve_table_handle_and_metadata(delta_spark, tmp_path, mode) -> None:
    table = _table(delta_spark, tmp_path / mode, native_check=True)
    before = table.detail().select("properties").first()["properties"]
    changes = _changes(delta_spark)
    files = render_generated_project(
        Apply,
        source_transform=f"{Apply.__module__}.Apply",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, Change]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        result = Apply(changes=changes, orders=table).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    assert result.orders is table
    assert sorted(tuple(row) for row in table.toDF().collect()) == [("1", "changed"), ("3", "new")]
    assert table.detail().select("properties").first()["properties"] == before


def test_missing_native_check_fails_before_mutation(delta_spark, tmp_path) -> None:
    table = _table(delta_spark, tmp_path / "missing", native_check=False)
    before = sorted(tuple(row) for row in table.toDF().collect())
    with pytest.raises(ValueError, match="missing CHECK valid_status"):
        Apply(changes=_changes(delta_spark), orders=table).run(session(delta_spark, execution_mode="online"))
    assert sorted(tuple(row) for row in table.toDF().collect()) == before


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_remaining_matched_and_unmatched_merge_actions(delta_spark, tmp_path, mode) -> None:
    table = _table(
        delta_spark,
        tmp_path / f"merge-families-{mode}",
        native_check=True,
        rows=(("1", "open"), ("2", "open"), ("3", "open")),
    )
    changes = _changes(
        delta_spark,
        (("1", "update"), ("2", "delete"), ("3", "replace"), ("4", "insert"), ("5", "insert-all")),
    )
    files = render_generated_project(
        MergeFamilies,
        source_transform=f"{MergeFamilies.__module__}.MergeFamilies",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, Change]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        MergeFamilies(changes=changes, orders=table).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    assert sorted(tuple(row) for row in table.toDF().collect()) == [
        ("1", "update"),
        ("3", "replace"),
        ("4", "insert"),
        ("5", "insert-all"),
    ]


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_unmatched_by_source_update_and_delete(delta_spark, tmp_path, mode) -> None:
    table = _table(
        delta_spark,
        tmp_path / f"by-source-{mode}",
        native_check=True,
        rows=(("1", "keep"), ("2", "remove")),
    )
    files = render_generated_project(
        MergeBySource,
        source_transform=f"{MergeBySource.__module__}.MergeBySource",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, Change]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        MergeBySource(changes=_changes(delta_spark, ()), orders=table).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    assert sorted(tuple(row) for row in table.toDF().collect()) == [("1", "stale")]


def test_check_match_modes_and_shape_preflight(delta_spark, tmp_path) -> None:
    table = _table(delta_spark, tmp_path / "name", native_check=True, check_sql="status <> 'blocked'")
    before = sorted(tuple(row) for row in table.toDF().collect())
    with pytest.raises(ValueError, match="CHECK valid_status differs"):
        InvalidUpdate(orders=table).run(session(delta_spark, execution_mode="online"))
    assert sorted(tuple(row) for row in table.toDF().collect()) == before
    NameOnly(orders=table).run(session(delta_spark, execution_mode="online"))
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    fresh_rows = sorted(tuple(row) for row in DeltaTable.forPath(delta_spark, str(tmp_path / "name")).toDF().collect())
    assert fresh_rows == [("1", "open")]

    unchecked = _table(delta_spark, tmp_path / "off", native_check=False)
    ChecksOff(orders=unchecked).run(session(delta_spark, execution_mode="online"))
    assert sorted(tuple(row) for row in DeltaTable.forPath(delta_spark, str(tmp_path / "off")).toDF().collect()) == [
        ("1", "open")
    ]
    delta_spark.sql(f"ALTER TABLE delta.`{tmp_path / 'off'}` ADD COLUMNS (extra STRING)")
    with pytest.raises(ValueError, match="different columns"):
        ChecksOff(orders=unchecked).run(session(delta_spark, execution_mode="online"))


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_native_check_rejects_invalid_update(delta_spark, tmp_path, mode) -> None:
    table = _table(delta_spark, tmp_path / f"native-check-{mode}", native_check=True)
    before = sorted(tuple(row) for row in table.toDF().collect())
    files = render_generated_project(
        InvalidUpdate,
        source_transform=f"{InvalidUpdate.__module__}.InvalidUpdate",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        with pytest.raises(Exception, match="valid_status"):
            InvalidUpdate(orders=table).run(session(delta_spark, execution_mode=mode, generated_package=PACKAGE))
    assert sorted(tuple(row) for row in table.toDF().collect()) == before


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_delta_input_reads_as_dataframe_relation(delta_spark, tmp_path, mode) -> None:
    table = _table(delta_spark, tmp_path / f"input-{mode}", native_check=True)
    files = render_generated_project(
        ReadOrders,
        source_transform=f"{ReadOrders.__module__}.ReadOrders",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        result = ReadOrders(orders=table).run(session(delta_spark, execution_mode=mode, generated_package=PACKAGE))
    assert sorted(tuple(row) for row in result.viewed.collect()) == [("1", "open"), ("2", "open")]


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_history_and_detail_reads_are_typed_and_use_runtime_limit(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"metadata-{mode}"
    table = _table(delta_spark, path, native_check=True)
    delta_spark.sql(f"UPDATE delta.`{path}` SET status = 'updated' WHERE id = '1'")
    run_session = session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
    one = ReadOrderMetadata(orders=table, limit=1)
    two = ReadOrderMetadata(orders=table, limit=2)
    assert run_session.compile(one).key == run_session.compile(two).key
    files = render_generated_project(
        ReadOrderMetadata,
        source_transform=f"{ReadOrderMetadata.__module__}.ReadOrderMetadata",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, OrderCommit, OrderDetail]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        one_result = one.run(run_session)
        two_result = two.run(run_session)
    assert len(one_result.commits.collect()) == 1
    assert len(two_result.commits.collect()) == 2
    assert one_result.commits.first()["operation"] == "UPDATE"
    detail = one_result.details.first()
    assert detail["format"] == "delta"
    assert detail["location"].endswith(str(path))
    assert DeltaTable.forPath(delta_spark, str(path)).toDF().count() == 2


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_restore_optimize_and_vacuum_effects_on_disposable_table(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    def run(subject, **inputs):
        if mode == "generated":
            files = render_generated_project(
                subject,
                source_transform=f"{subject.__module__}.{subject.__name__}",
                generated_package=PACKAGE,
                source_schema_modules={Order.__module__: [Order]},
            )
            with generated_project(tmp_path, PACKAGE, files):
                return subject(**inputs).run(session(delta_spark, execution_mode=mode, generated_package=PACKAGE))
        return subject(**inputs).run(session(delta_spark, execution_mode=mode))

    path = tmp_path / f"maintenance-{mode}"
    table = _table(delta_spark, path, native_check=True)
    original_version = table.history(1).first()["version"]
    delta_spark.sql(f"UPDATE delta.`{path}` SET status = 'changed' WHERE id = '1'")

    run(RestoreOrders, orders=table, version=original_version)
    reopened = DeltaTable.forPath(delta_spark, str(path))
    assert sorted(tuple(row) for row in reopened.toDF().collect()) == [("1", "open"), ("2", "open")]
    assert reopened.history(1).first()["operation"] == "RESTORE"

    before_rows = sorted(tuple(row) for row in reopened.toDF().collect())
    run(CompactOrders, orders=reopened)
    run(ZOrderOrders, orders=reopened)
    assert sorted(tuple(row) for row in DeltaTable.forPath(delta_spark, str(path)).toDF().collect()) == before_rows

    delta_spark.sql(f"DELETE FROM delta.`{path}` WHERE id = '2'")
    before_files = set(path.rglob("*.parquet"))
    assert before_files
    with pytest.raises(Exception, match="retention"):
        run(VacuumOrders, orders=reopened, retention=0.0)
    original_guard = delta_spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    delta_spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        run(VacuumOrders, orders=reopened, retention=0.0)
    finally:
        delta_spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", original_guard)
    after_files = set(path.rglob("*.parquet"))
    assert len(after_files) < len(before_files)
    assert DeltaTable.forPath(delta_spark, str(path)).toDF().count() == 1


def _layout_table(spark, path):
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    spark.sql(
        f"""
        CREATE TABLE delta.`{path}` (
            customer_id STRING NOT NULL,
            product_id STRING NOT NULL,
            order_date STRING NOT NULL
        ) USING DELTA PARTITIONED BY (order_date)
        TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '3')
    """
    )
    for batch in range(3):
        spark.sql(
            f"""INSERT INTO delta.`{path}` VALUES
            ('c{batch}', 'p2', '2026-10-07'), ('c{batch}', 'p1', '2026-10-08'),
            ('c{batch + 3}', 'p1', '2026-10-07'), ('c{batch + 3}', 'p2', '2026-10-08')"""
        )
    table = DeltaTable.forPath(spark, str(path))
    # Prove the fixture supplies statistics for both requested keys before maintenance.
    adds = [
        entry["add"]
        for log in sorted((path / "_delta_log").glob("*.json"))
        for line in log.read_text().splitlines()
        if "add" in (entry := json.loads(line))
    ]
    assert len(adds) >= 6
    for add in adds:
        statistics = json.loads(add["stats"])
        assert {"customer_id", "product_id"} <= statistics["minValues"].keys()
        assert {"customer_id", "product_id"} <= statistics["maxValues"].keys()
    return table


def _liquid_clustered_table(spark, path):
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    spark.sql(
        f"""CREATE TABLE delta.`{path}` (
            customer_id STRING NOT NULL,
            product_id STRING NOT NULL,
            order_date STRING NOT NULL
        ) USING DELTA"""
    )
    for batch in range(3):
        spark.sql(
            f"""INSERT INTO delta.`{path}` VALUES
            ('c{batch}', 'p2', '2026-10-07'), ('c{batch}', 'p1', '2026-10-08'),
            ('c{batch + 3}', 'p1', '2026-10-07'), ('c{batch + 3}', 'p2', '2026-10-08')"""
        )
    spark.sql(f"ALTER TABLE delta.`{path}` CLUSTER BY (order_date)")
    return DeltaTable.forPath(spark, str(path))


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_liquid_clustering_metadata_and_full_recluster_preserve_rows(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"liquid-full-{mode}"
    table = _liquid_clustered_table(delta_spark, path)
    before = sorted(tuple(row) for row in table.toDF().collect())
    detail = table.detail().first().asDict(recursive=True)
    assert detail["clusteringColumns"] == ["order_date"]
    assert "clustering" in {feature.casefold() for feature in detail["tableFeatures"]}
    typed_detail_files = render_generated_project(
        ReadLiquidOrderMetadata,
        source_transform=f"{ReadLiquidOrderMetadata.__module__}.ReadLiquidOrderMetadata",
        generated_package=PACKAGE,
        source_schema_modules={LayoutOrder.__module__: [LayoutOrder, OrderDetail, LiquidOrderDetail]},
    )
    with generated_project(tmp_path, PACKAGE, typed_detail_files):
        typed_detail = (
            ReadLiquidOrderMetadata(orders=table)
            .run(session(delta_spark, execution_mode=mode, generated_package=PACKAGE))
            .details.first()
        )
        assert typed_detail["clusteringColumns"] == ["order_date"]
    incremental_files = render_generated_project(
        IncrementalClusteredOrders,
        source_transform=f"{IncrementalClusteredOrders.__module__}.IncrementalClusteredOrders",
        generated_package=PACKAGE,
        source_schema_modules={LayoutOrder.__module__: [LayoutOrder]},
    )
    with generated_project(tmp_path, PACKAGE, incremental_files):
        IncrementalClusteredOrders(orders=table).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    assert sorted(tuple(row) for row in table.toDF().collect()) == before
    incremental_history = table.history(1).first()
    assert incremental_history["operation"] == "OPTIMIZE"
    assert str(incremental_history["operationParameters"]["isFull"]).casefold() == "false"
    delta_spark.sql(f"INSERT INTO delta.`{path}` VALUES ('c9', 'p9', '2026-10-09')")
    before_full = sorted([*before, ("c9", "p9", "2026-10-09")])
    files = render_generated_project(
        FullReclusterOrders,
        source_transform=f"{FullReclusterOrders.__module__}.FullReclusterOrders",
        generated_package=PACKAGE,
        source_schema_modules={LayoutOrder.__module__: [LayoutOrder]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        result = FullReclusterOrders(orders=table).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    assert result.orders is table
    reopened = DeltaTable.forPath(delta_spark, str(path))
    assert sorted(tuple(row) for row in reopened.toDF().collect()) == before_full
    history = reopened.history(1).first()
    assert history["operation"] == "OPTIMIZE"
    assert str(history["operationParameters"]["isFull"]).casefold() == "true"


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_liquid_clustered_tables_support_typed_row_reads_and_mutations(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"liquid-rows-{mode}"
    table = _liquid_clustered_table(delta_spark, path)
    run_session = session(delta_spark, execution_mode=mode, generated_package=PACKAGE)

    def run(subject, *, schema_modules=None, **inputs):
        if mode == "generated":
            files = render_generated_project(
                subject,
                source_transform=f"{subject.__module__}.{subject.__name__}",
                generated_package=PACKAGE,
                source_schema_modules=schema_modules or {LayoutOrder.__module__: [LayoutOrder]},
            )
            with generated_project(tmp_path, PACKAGE, files):
                return subject(**inputs).run(run_session)
        return subject(**inputs).run(run_session)

    original_rows = sorted(tuple(row) for row in table.toDF().collect())
    append_rows = delta_spark.createDataFrame(
        [("c9", "p9", "2026-10-09")],
        "customer_id string, product_id string, order_date string",
    )
    run(AppendLiquidOrders, rows=append_rows, orders=table)
    run(UpdateLiquidOrders, orders=table)
    merge_rows = delta_spark.createDataFrame(
        [("c0", "merged", "2026-10-07"), ("c8", "new", "2026-10-08")],
        "customer_id string, product_id string, order_date string",
    )
    run(MergeLiquidOrders, changes=merge_rows, orders=table)
    read = run(ReadLiquidOrders, orders=table).rows
    assert read.count() == len(original_rows) + 2
    assert read.where("customer_id = 'c0' AND product_id = 'merged'").count() == 1
    assert read.where("customer_id = 'c8' AND product_id = 'new'").count() == 1
    run(DeleteLiquidOrders, orders=table)
    final = DeltaTable.forPath(delta_spark, str(path)).toDF()
    assert final.where("customer_id = 'c0'").count() == 0
    assert final.count() == len(original_rows)


@pytest.mark.parametrize("subject", [OptimizeClusteredWhere, ZOrderClusteredOrders])
@pytest.mark.parametrize("mode", ["online", "generated"])
def test_liquid_clustering_rejects_where_and_zorder_before_commit(delta_spark, tmp_path, subject, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"liquid-invalid-{subject.__name__}-{mode}"
    table = _liquid_clustered_table(delta_spark, path)
    before_version = table.history(1).first()["version"]
    if mode == "generated":
        files = render_generated_project(
            subject,
            source_transform=f"{subject.__module__}.{subject.__name__}",
            generated_package=PACKAGE,
            source_schema_modules={LayoutOrder.__module__: [LayoutOrder]},
        )
        with generated_project(tmp_path, PACKAGE, files):
            with pytest.raises(ValueError, match="[Ll]iquid-clustered Delta tables"):
                subject(orders=table).run(session(delta_spark, execution_mode=mode, generated_package=PACKAGE))
    else:
        with pytest.raises(ValueError, match="[Ll]iquid-clustered Delta tables"):
            subject(orders=table).run(session(delta_spark, execution_mode=mode))
    reopened = DeltaTable.forPath(delta_spark, str(path))
    assert reopened.history(1).first()["version"] == before_version
    assert reopened.toDF().count() == 12


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_liquid_clustering_feature_still_blocks_zorder_after_keys_cleared(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"liquid-none-{mode}"
    _liquid_clustered_table(delta_spark, path)
    delta_spark.sql(f"ALTER TABLE delta.`{path}` CLUSTER BY NONE")
    current = DeltaTable.forPath(delta_spark, str(path))
    detail = current.detail().first().asDict(recursive=True)
    assert not detail["clusteringColumns"]
    assert "clustering" in {feature.casefold() for feature in detail["tableFeatures"]}
    before_version = current.history(1).first()["version"]
    if mode == "generated":
        files = render_generated_project(
            ZOrderClusteredOrders,
            source_transform=f"{ZOrderClusteredOrders.__module__}.ZOrderClusteredOrders",
            generated_package=PACKAGE,
            source_schema_modules={LayoutOrder.__module__: [LayoutOrder]},
        )
        with generated_project(tmp_path, PACKAGE, files):
            with pytest.raises(ValueError, match="[Ll]iquid-clustered Delta tables"):
                ZOrderClusteredOrders(orders=current).run(
                    session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
                )
    else:
        with pytest.raises(ValueError, match="[Ll]iquid-clustered Delta tables"):
            ZOrderClusteredOrders(orders=current).run(session(delta_spark, execution_mode=mode))
    assert DeltaTable.forPath(delta_spark, str(path)).history(1).first()["version"] == before_version


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize("scoped", [False, True], ids=["full-table", "partition"])
def test_zorder_multiple_keys_preserves_rows_and_partition_files(delta_spark, tmp_path, mode, scoped) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"zorder-{scoped}-{mode}"
    table = _layout_table(delta_spark, path)
    frame = table.toDF()
    before_rows = sorted(tuple(row) for row in frame.collect())
    before_schema = frame.schema
    before_files = set(frame.inputFiles())
    excluded = {name for name in before_files if "/order_date=2026-10-07/" in name}
    selected = before_files - excluded
    assert len(excluded) >= 3 and len(selected) >= 3

    subject = ZOrderPartition if scoped else ZOrderLayout
    inputs = {"orders": table, **({"selected_date": "2026-10-08"} if scoped else {})}
    files = render_generated_project(
        subject,
        source_transform=f"{subject.__module__}.{subject.__name__}",
        generated_package=PACKAGE,
        source_schema_modules={LayoutOrder.__module__: [LayoutOrder]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        result = subject(**inputs).run(session(delta_spark, execution_mode=mode, generated_package=PACKAGE))
    assert result.orders is table
    reopened = DeltaTable.forPath(delta_spark, str(path))
    after = reopened.toDF()
    assert sorted(tuple(row) for row in after.collect()) == before_rows
    assert after.schema == before_schema
    history = reopened.history(1).first()
    assert history["operation"] == "OPTIMIZE"
    assert json.loads(history["operationParameters"]["zOrderBy"]) == ["customer_id", "product_id"]
    after_files = set(after.inputFiles())
    assert not selected & after_files
    if scoped:
        assert {name for name in after_files if "/order_date=2026-10-07/" in name} == excluded
    else:
        assert not excluded & after_files


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_zorder_rejects_non_partition_filter_without_commit(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"invalid-zorder-{mode}"
    table = _layout_table(delta_spark, path)
    before_version = table.history(1).first()["version"]
    before_files = set(table.toDF().inputFiles())
    files = render_generated_project(
        ZOrderInvalidPartition,
        source_transform=f"{ZOrderInvalidPartition.__module__}.{ZOrderInvalidPartition.__name__}",
        generated_package=PACKAGE,
        source_schema_modules={LayoutOrder.__module__: [LayoutOrder]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        with pytest.raises(ValueError, match="only partition columns; invalid: customer_id"):
            ZOrderInvalidPartition(orders=table).run(
                session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
            )
    reopened = DeltaTable.forPath(delta_spark, str(path))
    assert reopened.history(1).first()["version"] == before_version
    assert set(reopened.toDF().inputFiles()) == before_files
    assert reopened.toDF().count() == 12


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_restore_can_return_a_prior_table_schema(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"restore-prior-schema-{mode}"
    table = _table(delta_spark, path, native_check=True)
    before_schema_change = table.history(1).first()["version"]
    delta_spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS (note STRING)")
    assert set(DeltaTable.forPath(delta_spark, str(path)).toDF().columns) == {"id", "status", "note"}

    files = render_generated_project(
        RestoreSchemaOrders,
        source_transform=f"{RestoreSchemaOrders.__module__}.RestoreSchemaOrders",
        generated_package=PACKAGE,
        source_schema_modules={Order.__module__: [Order, OrderV2]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        RestoreSchemaOrders(current_orders=table, version=before_schema_change).run(
            session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
        )
    restored = DeltaTable.forPath(delta_spark, str(path))
    assert set(restored.toDF().columns) == {"id", "status"}
    assert restored.history(1).first()["operation"] == "RESTORE"


def test_delta_column_feature_metadata_contract(delta_spark, tmp_path) -> None:
    if type(delta_spark).__module__.startswith("pyspark.sql.connect"):
        pytest.skip("Client JVM metadata inspection belongs to the ordinary PySpark regression lane")
    from delta.tables import DeltaTable, IdentityGenerator  # type: ignore[import-not-found]
    from pyspark.sql.types import LongType

    generated_path = tmp_path / "generated-column"
    DeltaTable.create(delta_spark).location(str(generated_path)).addColumn(
        "base", dataType=LongType(), nullable=False
    ).addColumn("derived", dataType=LongType(), generatedAlwaysAs="base + 1").execute()
    delta_spark.createDataFrame([(2,)], ["base"]).write.format("delta").mode("append").save(str(generated_path))
    generated_table = DeltaTable.forPath(delta_spark, str(generated_path)).toDF()
    from structure.plugin.pyspark.delta.runtime import validate_delta_table

    validate_delta_table(DeltaTable.forPath(delta_spark, str(generated_path)), GeneratedColumnSchema)
    with pytest.raises(ValueError, match="generated expression"):
        validate_delta_table(DeltaTable.forPath(delta_spark, str(generated_path)), WrongGeneratedColumnSchema)
    assert generated_table.schema["derived"].metadata == {}
    assert [(row.base, row.derived) for row in generated_table.collect()] == [(2, 3)]
    delta_log = delta_spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        delta_spark._jsparkSession,
        delta_spark._jvm.org.apache.hadoop.fs.Path(str(generated_path)),
    )
    generated_log_schema = json.loads(delta_log.unsafeVolatileSnapshot().metadata().schemaString())
    generated_fields = {field["name"]: field for field in generated_log_schema["fields"]}
    assert generated_fields["derived"]["metadata"]["delta.generationExpression"] == "base + 1"

    identity_path = tmp_path / "identity-column"
    identity_name = f"identity_{abs(hash(str(identity_path)))}"
    DeltaTable.create(delta_spark).tableName(identity_name).location(str(identity_path)).addColumn(
        "id", dataType=LongType(), generatedAlwaysAs=IdentityGenerator()
    ).addColumn("value", "STRING", nullable=False).execute()
    delta_spark.createDataFrame([("a",), ("b",)], ["value"]).write.format("delta").mode("append").save(
        str(identity_path)
    )
    identity_table = DeltaTable.forPath(delta_spark, str(identity_path)).toDF()
    validate_delta_table(DeltaTable.forPath(delta_spark, str(identity_path)), IdentityColumnSchema)
    with pytest.raises(ValueError, match="declared identity mode, start, and step"):
        validate_delta_table(DeltaTable.forPath(delta_spark, str(identity_path)), WrongIdentityColumnSchema)
    assert identity_table.schema["id"].metadata == {}
    assert sorted(row.id for row in identity_table.collect()) == [1, 2]
    identity_log = delta_spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        delta_spark._jsparkSession,
        delta_spark._jvm.org.apache.hadoop.fs.Path(str(identity_path)),
    )
    identity_log_schema = json.loads(identity_log.unsafeVolatileSnapshot().metadata().schemaString())
    identity_fields = {field["name"]: field for field in identity_log_schema["fields"]}
    assert identity_fields["id"]["metadata"] == {
        "delta.identity.highWaterMark": 2,
        "delta.identity.step": 1,
        "delta.identity.allowExplicitInsert": False,
        "delta.identity.start": 1,
    }

    default_path = tmp_path / "default-column"
    default_name = f"default_{abs(hash(str(default_path)))}"
    delta_spark.sql(
        f"CREATE TABLE {default_name} (id BIGINT NOT NULL, value STRING DEFAULT 'open') "
        "USING DELTA TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported') "
        f"LOCATION '{default_path}'"
    )
    delta_spark.sql(f"INSERT INTO {default_name} (id) VALUES (1)")
    default_table = DeltaTable.forPath(delta_spark, str(default_path)).toDF()
    validate_delta_table(DeltaTable.forPath(delta_spark, str(default_path)), DefaultColumnSchema)
    with pytest.raises(ValueError, match="declared default"):
        validate_delta_table(DeltaTable.forPath(delta_spark, str(default_path)), WrongDefaultColumnSchema)
    assert default_table.schema["value"].metadata.get("CURRENT_DEFAULT") == "'open'"
    assert [row.value for row in default_table.collect()] == ["open"]


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_declared_auto_columns_can_be_omitted_from_append_and_merge_writes(delta_spark, tmp_path, mode) -> None:
    from delta.tables import DeltaTable, IdentityGenerator  # type: ignore[import-not-found]
    from pyspark.sql.types import LongType

    generated_path = tmp_path / f"generated-write-{mode}"
    DeltaTable.create(delta_spark).location(str(generated_path)).addColumn(
        "base", dataType=LongType(), nullable=False
    ).addColumn("derived", dataType=LongType(), generatedAlwaysAs="base + 1").execute()
    generated_table = DeltaTable.forPath(delta_spark, str(generated_path))

    identity_path = tmp_path / f"identity-write-{mode}"
    identity_name = f"identity_write_{abs(hash(str(identity_path)))}"
    DeltaTable.create(delta_spark).tableName(identity_name).location(str(identity_path)).addColumn(
        "id", dataType=LongType(), generatedAlwaysAs=IdentityGenerator()
    ).addColumn("value", "STRING", nullable=False).execute()
    identity_table = DeltaTable.forPath(delta_spark, str(identity_path))

    default_path = tmp_path / f"default-write-{mode}"
    default_name = f"default_write_{abs(hash(str(default_path)))}"
    delta_spark.sql(
        f"CREATE TABLE {default_name} (id BIGINT NOT NULL, value STRING DEFAULT 'open') "
        "USING DELTA TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported') "
        f"LOCATION '{default_path}'"
    )
    default_table = DeltaTable.forPath(delta_spark, str(default_path))

    run_session = session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
    cases = (
        (
            AppendGeneratedColumn,
            GeneratedSource,
            delta_spark.createDataFrame([(2,)], ["base"]),
            generated_table,
        ),
        (
            InsertIdentityColumn,
            IdentitySource,
            delta_spark.createDataFrame([("new",)], ["value"]),
            identity_table,
        ),
        (
            InsertDefaultColumn,
            DefaultSource,
            delta_spark.createDataFrame([(1,)], ["id"]),
            default_table,
        ),
    )
    schema_modules = {
        Order.__module__: [
            GeneratedColumnSchema,
            IdentityColumnSchema,
            DefaultColumnSchema,
            GeneratedSource,
            IdentitySource,
            DefaultSource,
        ]
    }
    for subject, source_schema, rows, table in cases:
        if mode == "generated":
            source_transform = f"{subject.__module__}.{subject.__name__}"
            files = render_generated_project(
                subject,
                source_transform=source_transform,
                generated_package=PACKAGE,
                source_schema_modules=schema_modules,
            )
            with generated_project(tmp_path, PACKAGE, files):
                subject(rows=rows, orders=table).run(run_session)
        else:
            subject(rows=rows, orders=table).run(run_session)

    assert [(row.base, row.derived) for row in generated_table.toDF().collect()] == [(2, 3)]
    identity_ids = [row.id for row in identity_table.toDF().collect()]
    assert len(identity_ids) == 1 and identity_ids[0] > 0
    assert [(row.id, row.value) for row in default_table.toDF().collect()] == [(1, "open")]


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_connect_declaration_drift_delegates_values_to_delta(delta_spark, tmp_path, mode) -> None:
    if not type(delta_spark).__module__.startswith("pyspark.sql.connect"):
        pytest.skip("Declaration drift delegation is specific to Connect 4.1")
    from delta.tables import DeltaTable, IdentityGenerator  # type: ignore[import-not-found]
    from pyspark.sql.types import LongType

    generated_path = tmp_path / f"drift-generated-{mode}"
    DeltaTable.create(delta_spark).location(str(generated_path)).addColumn(
        "base", dataType=LongType(), nullable=False
    ).addColumn("derived", dataType=LongType(), generatedAlwaysAs="base + 1").execute()
    delta_spark.createDataFrame([(1,)], ["base"]).write.format("delta").mode("append").save(str(generated_path))
    generated = DeltaTable.forPath(delta_spark, str(generated_path))

    identity_path = tmp_path / f"drift-identity-{mode}"
    identity_name = f"identity_drift_{abs(hash(str(identity_path)))}"
    DeltaTable.create(delta_spark).tableName(identity_name).location(str(identity_path)).addColumn(
        "id", dataType=LongType(), generatedAlwaysAs=IdentityGenerator()
    ).addColumn("value", "STRING", nullable=False).execute()
    delta_spark.createDataFrame([("direct",)], ["value"]).write.format("delta").mode("append").save(str(identity_path))
    identity = DeltaTable.forPath(delta_spark, str(identity_path))

    default_path = tmp_path / f"drift-default-{mode}"
    default_name = f"default_drift_{abs(hash(str(default_path)))}"
    delta_spark.sql(
        f"CREATE TABLE {default_name} (id BIGINT NOT NULL, value STRING DEFAULT 'open') "
        "USING DELTA TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported') "
        f"LOCATION '{default_path}'"
    )
    delta_spark.createDataFrame([(1,)], ["id"]).write.format("delta").mode("append").save(str(default_path))
    default = DeltaTable.forPath(delta_spark, str(default_path))

    cases = (
        (AppendWrongGeneratedColumn, delta_spark.createDataFrame([(2,)], ["base"]), generated),
        (InsertWrongIdentityColumn, delta_spark.createDataFrame([("declared",)], ["value"]), identity),
        (InsertWrongDefaultColumn, delta_spark.createDataFrame([(2,)], ["id"]), default),
    )
    schema_modules = {
        Order.__module__: [
            WrongGeneratedColumnSchema,
            WrongIdentityColumnSchema,
            WrongDefaultColumnSchema,
            GeneratedSource,
            IdentitySource,
            DefaultSource,
        ]
    }
    run_session = session(delta_spark, execution_mode=mode, generated_package=PACKAGE)
    for subject, rows, table in cases:
        if mode == "generated":
            files = render_generated_project(
                subject,
                source_transform=f"{subject.__module__}.{subject.__name__}",
                generated_package=PACKAGE,
                source_schema_modules=schema_modules,
            )
            with generated_project(tmp_path, PACKAGE, files):
                subject(rows=rows, orders=table).run(run_session)
        else:
            subject(rows=rows, orders=table).run(run_session)

    assert sorted((row.base, row.derived) for row in generated.toDF().collect()) == [(1, 2), (2, 3)]
    identity_rows = {row.value: row.id for row in identity.toDF().collect()}
    assert identity_rows["direct"] > 0
    assert identity_rows["declared"] > identity_rows["direct"]
    assert sorted((row.id, row.value) for row in default.toDF().collect()) == [(1, "open"), (2, "open")]


def test_connect_binding_request_count_against_direct_delta_write(delta_spark, tmp_path, monkeypatch) -> None:
    if not type(delta_spark).__module__.startswith("pyspark.sql.connect"):
        pytest.skip("Remote request counts are specific to Connect 4.1")
    from time import perf_counter

    from delta.tables import DeltaTable  # type: ignore[import-not-found]
    from pyspark.sql.connect.client.core import SparkConnectClient
    from pyspark.sql.types import LongType

    from structure.plugin.pyspark.delta import runtime

    path = tmp_path / "binding-requests"
    DeltaTable.create(delta_spark).location(str(path)).addColumn("base", dataType=LongType(), nullable=False).addColumn(
        "derived", dataType=LongType(), generatedAlwaysAs="base + 1"
    ).execute()
    table = DeltaTable.forPath(delta_spark, str(path))
    rows = delta_spark.createDataFrame([(3,)], ["base"])
    calls = {"execute": 0, "analyze": 0, "refresh": 0}

    def count(name, original):
        def wrapped(*args, **kwargs):
            calls[name] += 1
            return original(*args, **kwargs)

        return wrapped

    with monkeypatch.context() as patch:
        patch.setattr(
            SparkConnectClient,
            "_execute_and_fetch_as_iterator",
            count("execute", SparkConnectClient._execute_and_fetch_as_iterator),
        )
        patch.setattr(SparkConnectClient, "_analyze", count("analyze", SparkConnectClient._analyze))
        patch.setattr(runtime, "fresh_delta_frame", count("refresh", runtime.fresh_delta_frame))
        patch.setattr(
            runtime, "_delta_log_field_metadata", lambda _: pytest.fail("Connect requested client JVM metadata")
        )
        started = perf_counter()
        runtime.validated_delta_frame(table, WrongGeneratedColumnSchema)
        binding_seconds = perf_counter() - started
        binding_calls = dict(calls)
        assert binding_calls["refresh"] == 1

        calls.update(execute=0, analyze=0, refresh=0)
        started = perf_counter()
        rows.write.format("delta").mode("append").save(str(path))
        direct_seconds = perf_counter() - started
        direct_calls = dict(calls)

    assert [(row.base, row.derived) for row in table.toDF().collect()] == [(3, 4)]
    print(
        f"Connect binding: {binding_calls}, {binding_seconds:.3f}s; "
        f"direct Delta append: {direct_calls}, {direct_seconds:.3f}s"
    )


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize(
    ("subject", "expected"),
    [
        (ReplaceCleanup, {("A", "open"), ("B", "legacy")}),
        (ExtendCleanup, {("A", "open")}),
    ],
)
def test_table_inheritance_overrides_and_super_match_live_rows(delta_spark, tmp_path, mode, subject, expected) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / f"inherit-{subject.__name__}-{mode}"
    table = _table(
        delta_spark,
        path,
        native_check=True,
        rows=(("A", "open"), ("B", "legacy"), ("C", "archived")),
    )
    package = f"tests.generated_delta_{subject.__name__.lower()}"
    files = render_generated_project(
        subject,
        source_transform=f"{subject.__module__}.{subject.__name__}",
        generated_package=package,
        source_schema_modules={Order.__module__: [Order]},
    )
    try:
        with generated_project(tmp_path, package, files):
            result = subject(orders=table).run(session(delta_spark, execution_mode=mode, generated_package=package))
        assert result.orders is table
        rows = DeltaTable.forPath(delta_spark, str(path)).toDF().collect()
        assert {(row.id, row.status) for row in rows} == expected
    finally:
        shutil.rmtree(str(path), ignore_errors=True)


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_composed_delta_stages_read_after_the_parent_commit(delta_spark, tmp_path, mode) -> None:
    path = tmp_path / f"compose-table-{mode}"
    table = _table(
        delta_spark,
        path,
        native_check=True,
        rows=(("A", "open"), ("B", "legacy"), ("C", "archived")),
    )
    package = f"tests.generated_delta_compose_{mode}"
    files = render_generated_project(
        CleanupThenRead,
        source_transform=f"{CleanupThenRead.__module__}.{CleanupThenRead.__name__}",
        generated_package=package,
        source_schema_modules={Order.__module__: [Order]},
    )
    try:
        with generated_project(tmp_path, package, files):
            result = CleanupThenRead(orders=table).run(
                session(delta_spark, execution_mode=mode, generated_package=package)
            )
        assert {(row.id, row.status) for row in result.selected.collect()} == {
            ("A", "open"),
            ("C", "archived"),
        }
    finally:
        shutil.rmtree(str(path), ignore_errors=True)
