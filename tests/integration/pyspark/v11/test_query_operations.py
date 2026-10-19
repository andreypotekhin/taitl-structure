from __future__ import annotations

import importlib
from typing import cast

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, special, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import boolean, lateral_join, long, named_struct, string
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.joins.Join import Join
from structure.plugin.pyspark.dsl.RelationQuery import RelationQuery

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark41", "spark-connect41"},
        reason="V11 typed relation queries require the PySpark 4.1 parity profile",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_query_operations"
PACKAGE = "integration_v11_query_operations_generated"


class StoreInput(Schema):
    store_id = string(nullable=False)
    min_quantity = long(nullable=False)


class InventoryInput(Schema):
    inventory_id = long(nullable=False)
    store_id = string(nullable=True)
    status = string(nullable=False)
    item_name = string(nullable=False)
    quantity = long(nullable=False)


class QuerySummary(Schema):
    store_id = string(nullable=False)
    has_clearance = boolean(nullable=False)
    has_stock = boolean(nullable=True)
    pair_match = boolean(nullable=True)
    no_results = boolean(nullable=True)


class InventoryMatch(Schema):
    store_id = string(nullable=True)
    inventory_id = long(nullable=False)
    item_name = string(nullable=False)
    quantity = long(nullable=False)


class StoreItem(Schema):
    store_id = string(nullable=False)
    inventory_id = long(nullable=True)
    item_name = string(nullable=True)
    quantity = long(nullable=True)


@transform
class SummarizeStoreInventory(Transform):
    stores = input(StoreInput)
    inventory = input(InventoryInput)
    summary = output(QuerySummary)

    @special(type="expr")
    def is_clearance(status):
        return status == "Clearance"

    def summarize(self, store: StoreInput, inventory: InventoryInput) -> QuerySummary:
        return QuerySummary(
            store_id=store.store_id,
            has_clearance=cast(
                RelationQuery,
                cast(InputScope, inventory).where(
                    (inventory.store_id == store.store_id.outer()) & self.is_clearance(inventory.status)
                ),
            ).exists(),
            has_stock=store.store_id.isin(cast(RelationQuery, cast(InputScope, inventory).project(["store_id"]))),
            pair_match=named_struct(
                "store_id", store.store_id, "quantity", store.min_quantity
            ).isin(cast(RelationQuery, cast(InputScope, inventory).project(["store_id", "quantity"]))),
            no_results=store.store_id.isin(
                cast(
                    RelationQuery,
                    cast(InputScope, inventory)
                    .where(inventory.status == "__no_such_status__")
                    .project(["store_id"]),
                )
            ),
        )


@transform
class SelectStoreInventory(Transform):
    stores = input(StoreInput)
    inventory = input(InventoryInput)
    matches = output(StoreItem)

    @special(type="expr")
    def is_clearance(status):
        return status == "Clearance"

    def select(self, store: StoreInput, inventory: InventoryInput) -> StoreItem:
        query = (
            cast(RelationQuery, cast(InputScope, inventory).where(
                (inventory.store_id == store.store_id.outer()) & self.is_clearance(inventory.status)
            ))
            .order_by(inventory.quantity.desc())
            .limit(2)
            .project(InventoryMatch)
        )
        joined = lateral_join(
            query,
            on=cast(Expression, query.quantity) >= cast(Expression, store.min_quantity),
            how=Join.LEFT,
        )
        return StoreItem(
            store_id=store.store_id,
            inventory_id=joined.inventory_id,
            item_name=joined.item_name,
            quantity=joined.quantity,
        )


def _generated_files(transform_class):
    files = render_generated_project(
        transform_class,
        source_transform=f"{SOURCE_MODULE}.{transform_class.__name__}",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StoreInput, InventoryInput, QuerySummary, InventoryMatch, StoreItem]},
    )
    source = "\n".join(files.values())
    assert ".outer(" in source
    assert "Clearance" in source
    assert not any(token in source for token in ("SparkContext", "sparkContext", "_jdf", "_jvm", ".rdd"))
    return files


def _inputs(spark, schemas):
    stores = spark.createDataFrame(
        [("a", 50), ("b", 10), ("missing", 1)], schemas.STORE_INPUT_SCHEMA
    )
    inventory = spark.createDataFrame(
        [
            (1, "a", "Clearance", "top", 100),
            (2, "a", "Clearance", "below-threshold", 20),
            (3, "a", "Regular", "regular", 500),
            (7, "a", "Regular", "match-minimum", 50),
            (4, "b", "Clearance", "large", 30),
            (5, "b", "Clearance", "large", 30),
            (6, None, "Clearance", "orphan", 1),
        ],
        schemas.INVENTORY_INPUT_SCHEMA,
    )
    return stores, inventory


def test_native_exists_and_in_queries_match_online_generated_and_spark(spark, tmp_path) -> None:
    from pyspark.sql import functions as F

    files = _generated_files(SummarizeStoreInventory)
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_query_operations")
        stores, inventory = _inputs(spark, schemas)
        online = SummarizeStoreInventory(stores=stores, inventory=inventory).run(
            session(spark, execution_mode="online")
        ).summary
        generated = SummarizeStoreInventory(stores=stores, inventory=inventory).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).summary
        stores_alias = stores.alias("store")
        inventory_alias = inventory.alias("inventory")
        native_exists = inventory_alias.where(
            (F.col("inventory.store_id") == F.col("store.store_id").outer())
            & (F.col("inventory.status") == F.lit("Clearance"))
        ).exists()
        native = stores_alias.select(
            F.col("store.store_id").alias("store_id"),
            native_exists.alias("has_clearance"),
            F.col("store.store_id").isin(inventory.select("store_id")).alias("has_stock"),
            F.struct(
                F.col("store.store_id"), F.col("store.min_quantity").alias("quantity")
            ).isin(inventory.select("store_id", "quantity")).alias("pair_match"),
            F.col("store.store_id")
            .isin(inventory.where(F.col("status") == "__no_such_status__").select("store_id"))
            .alias("no_results"),
        )

        assert_online_generated_parity(lambda: online, lambda: generated)
        assert online.schema == generated.schema == native.schema
        expected = {
            "a": (True, True, True, False),
            "b": (True, True, False, False),
            "missing": (False, False, False, False),
        }
        for frame in (online, generated, native):
            actual = {
                row["store_id"]: (
                    row["has_clearance"],
                    row["has_stock"],
                    row["pair_match"],
                    row["no_results"],
                )
                for row in rows(frame, "store_id")
            }
            assert actual == expected


def test_lateral_join_filters_after_ordered_inner_limit(spark, tmp_path) -> None:
    files = _generated_files(SelectStoreInventory)
    generated_source = "\n".join(files.values())
    assert ".lateralJoin(" in generated_source
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_query_operations")
        stores, inventory = _inputs(spark, schemas)
        online = SelectStoreInventory(stores=stores, inventory=inventory).run(
            session(spark, execution_mode="online")
        ).matches
        generated = SelectStoreInventory(stores=stores, inventory=inventory).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).matches
        from pyspark.sql import functions as F

        store_alias = stores.alias("store")
        inventory_alias = inventory.alias("inventory")
        query = (
            inventory_alias.where(
                (F.col("inventory.store_id") == F.col("store.store_id").outer())
                & (F.col("inventory.status") == "Clearance")
            )
            .orderBy(F.col("inventory.quantity").desc())
            .limit(2)
            .select("store_id", "inventory_id", "item_name", "quantity")
        )
        native = store_alias.lateralJoin(
            query,
            on=F.col("quantity") >= F.col("store.min_quantity"),
            how="left",
        ).select(
            F.col("store.store_id").alias("store_id"),
            "inventory_id",
            "item_name",
            "quantity",
        )

        assert_online_generated_parity(lambda: online, lambda: generated)
        assert online.schema == generated.schema == native.schema
        expected = [
            ("a", 1, "top", 100),
            ("b", 4, "large", 30),
            ("b", 5, "large", 30),
            ("missing", None, None, None),
        ]
        for frame in (online, generated, native):
            actual = rows(frame, "store_id", "inventory_id")
            assert [
                (row["store_id"], row["inventory_id"], row["item_name"], row["quantity"]) for row in actual
            ] == expected
