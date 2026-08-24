"""Reproduce driver heap growth from repeatedly reused Spark lineage.

This example does not depend on Structure or the Search example.  Each round
aliases the current relation twice, joins the aliases, creates a canonical
pair, and emits both directions.  The default ``union`` mode materializes a
reverse branch and unions it with the canonical branch.  The ``explode`` mode
emits both rows from one projection, isolating the union-branch multiplier.
Reusing the current DataFrame on both sides of the join still duplicates the
entire preceding lineage.

Run it in the PySpark integration image, for example:

    python docs/troubleshooting/memory/spark_driver_heap_oom.py --rounds 8
    python docs/troubleshooting/memory/spark_driver_heap_oom.py --rounds 12 --directions explode

The script prints progress while constructing the plan.  With a small driver
heap, the JVM may fail during plan construction before the final count().
"""

from __future__ import annotations

import argparse
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_round(current: DataFrame, round_number: int, *, directions: str = "union") -> DataFrame:
    """Add one self-join round and emit both result directions."""

    left_name = f"left_{round_number}"
    right_name = f"right_{round_number}"
    left = current.alias(left_name)
    right = current.alias(right_name)
    joined = left.join(
        right,
        F.col(f"{left_name}.right_id") == F.col(f"{right_name}.left_id"),
        "inner",
    )
    canonical = joined.where(F.col(f"{left_name}.left_id") < F.col(f"{right_name}.right_id")).select(
        F.col(f"{left_name}.left_id").alias("left_id"),
        F.col(f"{right_name}.right_id").alias("right_id"),
    )
    reverse = canonical.select(
        F.col("right_id").alias("left_id"),
        F.col("left_id").alias("right_id"),
    )
    if directions == "union":
        return canonical.unionByName(reverse)
    if directions == "explode":
        pair = F.explode(
            F.array(
                F.struct(
                    F.col("left_id").alias("left_id"),
                    F.col("right_id").alias("right_id"),
                ),
                F.struct(
                    F.col("right_id").alias("left_id"),
                    F.col("left_id").alias("right_id"),
                ),
            )
        ).alias("pair")
        return canonical.select(pair).select("pair.*")
    raise ValueError(f"Unknown direction expansion: {directions}")


def logical_plan_size(dataframe: DataFrame) -> int:
    """Return the UTF-16 length of Spark's unresolved logical-plan text."""

    plan = dataframe._jdf.queryExecution().logical().toString()
    return len(plan)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=8)
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=0,
        help="checkpoint after this many rounds to cut the accumulated logical lineage",
    )
    parser.add_argument(
        "--view-every",
        type=int,
        default=0,
        help="replace the current frame with a temporary-view reference after this many rounds",
    )
    parser.add_argument(
        "--directions",
        choices=("union", "explode"),
        default="union",
        help="emit both directions with two union branches or one explode projection",
    )
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    if args.checkpoint_every < 0:
        parser.error("--checkpoint-every cannot be negative")
    if args.view_every < 0:
        parser.error("--view-every cannot be negative")
    if args.checkpoint_every and args.view_every:
        parser.error("choose at most one of --checkpoint-every and --view-every")

    spark = (
        SparkSession.builder.appName("spark-driver-heap-oom-reproducer")
        .master(os.environ.get("STRUCTURE_SPARK_MASTER", "local[2]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        current = spark.range(2).select(
            F.col("id").alias("left_id"),
            (F.col("id") + 1).alias("right_id"),
        )
        started = time.perf_counter()
        for round_number in range(args.rounds):
            current = build_round(current, round_number, directions=args.directions)
            if args.checkpoint_every and (round_number + 1) % args.checkpoint_every == 0:
                current = current.localCheckpoint(eager=True)
            if args.view_every and (round_number + 1) % args.view_every == 0:
                view = f"structure_lineage_round_{round_number}"
                current.createOrReplaceTempView(view)
                current = spark.table(view)
            elapsed = time.perf_counter() - started
            print(
                f"round={round_number + 1} plan_chars={logical_plan_size(current)} " f"elapsed={elapsed:.1f}s",
                flush=True,
            )

        print("forcing Spark analysis and execution with count()", flush=True)
        print(f"rows={current.count()}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
