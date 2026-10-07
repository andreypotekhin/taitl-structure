"""Native Iceberg SQL and Spark writer evidence for the pinned runtime matrix."""

from __future__ import annotations

from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


def test_native_sql_mutations_and_snapshot_reads(spark, integration_shared_dir) -> None:
    name = f"structure_iceberg.default.orders_{uuid4().hex}"
    spark.sql(
        f"CREATE TABLE {name} (id STRING NOT NULL, status STRING) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    try:
        spark.sql(f"INSERT INTO {name} VALUES ('A', 'new'), ('B', 'new')")
        snapshot = spark.sql(f"SELECT snapshot_id FROM {name}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
        spark.sql(f"UPDATE {name} SET status = 'paid' WHERE id = 'A'")
        spark.sql(f"DELETE FROM {name} WHERE id = 'B'")
        spark.sql(f"INSERT INTO {name} VALUES ('C', 'new')")
        spark.sql(
            f"MERGE INTO {name} t USING (SELECT 'A' id, 'shipped' status "
            "UNION ALL SELECT 'D', 'new') s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET status = s.status "
            "WHEN NOT MATCHED THEN INSERT (id, status) VALUES (s.id, s.status)"
        )

        assert {(row.id, row.status) for row in spark.table(name).collect()} == {
            ("A", "shipped"),
            ("C", "new"),
            ("D", "new"),
        }
        assert {(row.id, row.status) for row in spark.sql(f"SELECT * FROM {name} VERSION AS OF {snapshot}").collect()} == {
            ("A", "new"),
            ("B", "new"),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_writer_v2_append_schema_merge_requires_table_property(spark) -> None:
    from pyspark.sql import functions as F

    name = f"structure_iceberg.default.evolving_{uuid4().hex}"
    spark.sql(
        f"CREATE TABLE {name} (id STRING NOT NULL) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    source = spark.createDataFrame([("A", "note")], "id STRING, note STRING")
    try:
        with pytest.raises(Exception):
            source.writeTo(name).option("mergeSchema", "true").append()
        assert spark.table(name).columns == ["id"]

        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
        source.writeTo(name).option("mergeSchema", "true").append()
        assert spark.table(name).columns == ["id", "note"]
        assert spark.table(name).select(F.col("note")).first()[0] == "note"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_native_procedure_results_and_history(spark) -> None:
    name = f"structure_iceberg.default.maintenance_{uuid4().hex}"
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        rows = spark.sql(f"CALL structure_iceberg.system.rewrite_data_files(table => 'default.{name.rsplit('.', 1)[1]}')")
        assert rows.columns
        assert spark.sql(f"SELECT count(*) FROM {name}").first()[0] == 1
        assert spark.sql(f"SELECT count(*) FROM {name}.history").first()[0] >= 1
        assert spark.sql(f"SELECT count(*) FROM {name}.snapshots").first()[0] >= 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
