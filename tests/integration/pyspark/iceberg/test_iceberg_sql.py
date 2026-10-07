"""Structure SQL execution against native Iceberg tables."""

from __future__ import annotations

from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import session

from structure import Schema, Transform, input, output
from structure.plugin.pyspark import SqlCommandResult, sql, string

pytestmark = pytest.mark.integration


class Order(Schema):
    id = string(nullable=False)
    status = string()


def test_typed_sql_appends_to_iceberg_and_returns_repeatable_receipt(spark) -> None:
    name = f"structure_iceberg.default.sql_orders_{uuid4().hex}"
    spark.sql(f"CREATE TABLE {name} (id STRING NOT NULL, status STRING) USING iceberg")
    try:
        source = spark.createDataFrame([("A", "new"), ("B", "paid")], "id STRING, status STRING")

        class AppendOrders(Transform):
            TARGET = name
            orders = input(Order)
            commands = output(SqlCommandResult)

            def append(self, order: Order) -> SqlCommandResult:
                return sql(
                    "INSERT INTO {target} SELECT id, status FROM {orders}",
                    relations={"target": self.TARGET, "orders": order},
                    to=SqlCommandResult,
                    label="append-orders",
                )

        result = AppendOrders(orders=source).run(session(spark, execution_mode="online"))
        receipt = result.commands
        assert receipt.collect() == receipt.collect()
        assert {(row.id, row.status) for row in spark.table(name).collect()} == {
            ("A", "new"),
            ("B", "paid"),
        }
        assert result.commands.first().label == "append-orders"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
