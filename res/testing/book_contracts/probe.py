"""Reproduce optimizer elimination without involving Structure's compiler."""

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
frame = spark.createDataFrame([(1,), (1,)], "id long")
guard = frame.agg(F.count("*").alias("n")).select(
    F.assert_true(F.col("n") == 1, "expected one row").alias("guard")
)
discarded = guard.crossJoin(frame).drop("guard")
print("DISCARDED PLAN", discarded._jdf.queryExecution().optimizedPlan().toString())
print("DISCARDED RESULT", discarded.collect())
lazy = frame.crossJoin(guard).where(F.col("guard").isNull()).drop("guard")
print("EMPTY LAZY RESULT", lazy.where(F.lit(False)).collect())
try:
    guard.first()
except Exception as error:
    print("BOUNDARY ACTION REJECTED", "expected one row" in str(error))
finally:
    spark.stop()
