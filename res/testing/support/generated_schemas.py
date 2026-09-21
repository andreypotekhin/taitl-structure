"""Run directly to reproduce missing schema constants in programmatic generation."""

from structure import MemoryStorage, Schema, StructureConfig, StructureSession, Transform, input, output
from structure.plugin.pyspark import string


class Row(Schema):
    id = string(nullable=False)


class CopyRows(Transform):
    rows = input(Row)
    copied = output(Row)

    def copy(self, row: Row) -> Row:
        return row


def main():
    import platform

    import pyspark
    from pyspark.sql import SparkSession, types as T

    print(f"Python {platform.python_version()}, PySpark {pyspark.__version__}", flush=True)
    spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    config = StructureConfig.create(execution_mode="generated", generated_package="support_generated")
    storage = MemoryStorage()
    session = StructureSession(spark=spark, config=config, storage=storage)
    try:
        CopyRows.generate(config=config, storage=storage)
        rows = spark.createDataFrame([("one",)], T.StructType([T.StructField("id", T.StringType(), False)]))
        print(CopyRows(rows=rows).run(session).copied.collect())
    finally:
        session.close()
        spark.stop()


if __name__ == "__main__":
    main()
