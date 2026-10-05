"""Disposable native Delta table evidence for transform-owned mutations."""

from __future__ import annotations

from pathlib import Path

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session

from structure import Schema, Transform, input, output, step, transform
from structure.plugin.pyspark import (
    check,
    delta_append,
    delta_delete,
    delta_input,
    delta_merge,
    delta_output,
    delta_update,
    string,
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


@transform
class Apply(Transform):
    changes = input(Change)
    orders = delta_output(Order)

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
    orders = delta_output(Order)

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
    orders = delta_output(Order)

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
    orders = delta_output(Order)

    @step(input=orders, output=orders)
    def delete(self, order: Order) -> None:
        delta_delete(order, where=order.id == "2")


@transform(delta_check_match="off")
class ChecksOff(Transform):
    orders = delta_output(Order)

    @step(input=orders, output=orders)
    def delete(self, order: Order) -> None:
        delta_delete(order, where=order.id == "2")


@transform
class InvalidUpdate(Transform):
    orders = delta_output(Order)

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
class EvolvingMerge(Transform):
    changes = input(ChangeV2)
    current_orders = delta_input(Order)
    orders = delta_output(OrderV2)

    def merge(self, change: ChangeV2, order: Order) -> OrderV2:
        return (
            delta_merge(order, change, on=order.id == change.id)
            .with_schema_evolution()
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
        return delta_append(order, change).with_schema_evolution().execute()


@pytest.fixture
def delta_spark():
    pyspark = pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    if pyspark.__version__ != "4.1.0":
        pytest.skip("Delta transform evidence is pinned to PySpark 4.1.0")
    from delta import configure_spark_with_delta_pip  # type: ignore[import-not-found]
    from pyspark.sql import SparkSession

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
