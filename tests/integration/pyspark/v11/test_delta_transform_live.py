"""Disposable native Delta table evidence for transform-owned mutations."""

from __future__ import annotations

import json
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


class DefaultColumnSchema(Schema):
    id = long(nullable=False)
    value = string()
    delta_columns = (delta_default(value, value="open"),)


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
        "pyspark40": ("4.0.0", "4.1.0"),
        "pyspark41": ("4.1.0", "4.1.0"),
    }
    expected = expected_versions.get(backend)
    if expected is None:
        pytest.skip("Delta transform evidence runs only in classic PySpark 3.5, 4.0, and 4.1 lanes")
    from importlib.metadata import version

    actual = (pyspark.__version__, version("delta-spark"))
    if actual != expected:
        pytest.fail(f"Delta evidence for {backend} requires PySpark/Delta {expected}, got {actual}")
    from delta import configure_spark_with_delta_pip  # type: ignore[import-not-found]
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession() or getattr(SparkSession, "_instantiatedSession", None)
    if active is not None:
        active.stop()
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("structure-delta-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


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


def test_remaining_matched_and_unmatched_merge_actions(delta_spark, tmp_path) -> None:
    table = _table(
        delta_spark,
        tmp_path / "merge-families",
        native_check=True,
        rows=(("1", "open"), ("2", "open"), ("3", "open")),
    )
    changes = _changes(
        delta_spark,
        (("1", "update"), ("2", "delete"), ("3", "replace"), ("4", "insert"), ("5", "insert-all")),
    )
    MergeFamilies(changes=changes, orders=table).run(session(delta_spark, execution_mode="online"))
    assert sorted(tuple(row) for row in table.toDF().collect()) == [
        ("1", "update"),
        ("3", "replace"),
        ("4", "insert"),
        ("5", "insert-all"),
    ]


def test_unmatched_by_source_update_and_delete(delta_spark, tmp_path) -> None:
    table = _table(
        delta_spark,
        tmp_path / "by-source",
        native_check=True,
        rows=(("1", "keep"), ("2", "remove")),
    )
    MergeBySource(changes=_changes(delta_spark, ()), orders=table).run(session(delta_spark, execution_mode="online"))
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


def test_native_check_rejects_invalid_update(delta_spark, tmp_path) -> None:
    table = _table(delta_spark, tmp_path / "native-check", native_check=True)
    before = sorted(tuple(row) for row in table.toDF().collect())
    with pytest.raises(Exception, match="valid_status"):
        InvalidUpdate(orders=table).run(session(delta_spark, execution_mode="online"))
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


def test_restore_optimize_and_vacuum_effects_on_disposable_table(delta_spark, tmp_path) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / "maintenance"
    table = _table(delta_spark, path, native_check=True)
    original_version = table.history(1).first()["version"]
    delta_spark.sql(f"UPDATE delta.`{path}` SET status = 'changed' WHERE id = '1'")

    RestoreOrders(orders=table, version=original_version).run(session(delta_spark, execution_mode="online"))
    reopened = DeltaTable.forPath(delta_spark, str(path))
    assert sorted(tuple(row) for row in reopened.toDF().collect()) == [("1", "open"), ("2", "open")]
    assert reopened.history(1).first()["operation"] == "RESTORE"

    before_rows = sorted(tuple(row) for row in reopened.toDF().collect())
    CompactOrders(orders=reopened).run(session(delta_spark, execution_mode="online"))
    ZOrderOrders(orders=reopened).run(session(delta_spark, execution_mode="online"))
    assert sorted(tuple(row) for row in DeltaTable.forPath(delta_spark, str(path)).toDF().collect()) == before_rows

    delta_spark.sql(f"DELETE FROM delta.`{path}` WHERE id = '2'")
    before_files = set(path.rglob("*.parquet"))
    assert before_files
    with pytest.raises(Exception, match="retention"):
        VacuumOrders(orders=reopened, retention=0.0).run(session(delta_spark, execution_mode="online"))
    original_guard = delta_spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    delta_spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        VacuumOrders(orders=reopened, retention=0.0).run(session(delta_spark, execution_mode="online"))
    finally:
        delta_spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", original_guard)
    after_files = set(path.rglob("*.parquet"))
    assert len(after_files) < len(before_files)
    assert DeltaTable.forPath(delta_spark, str(path)).toDF().count() == 1


def test_restore_can_return_a_prior_table_schema(delta_spark, tmp_path) -> None:
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    path = tmp_path / "restore-prior-schema"
    table = _table(delta_spark, path, native_check=True)
    before_schema_change = table.history(1).first()["version"]
    delta_spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS (note STRING)")
    assert set(DeltaTable.forPath(delta_spark, str(path)).toDF().columns) == {"id", "status", "note"}

    RestoreSchemaOrders(current_orders=table, version=before_schema_change).run(
        session(delta_spark, execution_mode="online")
    )
    restored = DeltaTable.forPath(delta_spark, str(path))
    assert set(restored.toDF().columns) == {"id", "status"}
    assert restored.history(1).first()["operation"] == "RESTORE"


def test_delta_column_feature_metadata_contract(delta_spark, tmp_path) -> None:
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
