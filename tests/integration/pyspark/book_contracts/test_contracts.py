import os
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from testing.book_contracts import transforms as model
from testing.book_contracts.cache_transforms import ReleaseOrders

from structure import MemoryStorage, StructureConfig, StructureSession
from structure.plugin.pyspark import PySpark

pytestmark = pytest.mark.integration


@contextmanager
def run(spark, transform, mode, rows, *, options=None, **inputs):
    from pyspark.sql import types as T

    schema = PySpark.schema.materialize()(transform.orders.schema, types=T)
    variant = "spark-connect" if os.environ.get("STRUCTURE_SPARK_REMOTE") else "ordinary"
    config = StructureConfig.create(
        execution_mode=mode, generated_package="book_contract_generated",
        plugin={"pyspark": {"variant": variant, **(options or {})}}
    )
    storage = MemoryStorage()
    session = StructureSession(spark=spark, config=config, storage=storage)
    try:
        if mode == "generated":
            transform.generate(config=config, storage=storage)
        yield transform(orders=spark.createDataFrame(rows, schema), **inputs).run(session).checked
    finally:
        session.close()


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize("action", ["collect", "count", "project", "write"])
@pytest.mark.parametrize(
    "transform,rows,code",
    [
        (model.UniqueOrders, [("a", 1, 2), ("a", 1, 3)], "REL-E0702"),
        (model.PositiveOrders, [("a", 1, -1)], "REL-E0703"),
        (model.LatestOrders, [("a", 2, 35), ("a", 2, 99), ("a", 1, 20)], "tie"),
    ],
)
def test_invalid_data(spark, mode, action, transform, rows, code, tmp_path):
    with pytest.raises(Exception, match=code):
        with run(spark, transform, mode, rows) as result:
            if action == "project":
                result.select("id").collect()
            elif action == "write":
                result.write.mode("overwrite").parquet(str(tmp_path / "rejected"))
            else:
                getattr(result, action)()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_reference_rejection(spark, mode):
    from pyspark.sql import types as T

    reference = spark.createDataFrame([("b",)], T.StructType([T.StructField("id", T.StringType(), False)]))
    with pytest.raises(Exception, match="REL-E0704"):
        with run(spark, model.ReferencedOrders, mode, [("a", 1, 2)], references=reference) as result:
            result.collect()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_temporal_overlap(spark, mode):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("start", T.LongType(), False),
            T.StructField("end", T.LongType(), True),
            T.StructField("price", T.LongType(), False),
        ]
    )
    prices = spark.createDataFrame([("a", 1, 5, 40), ("a", 5, 10, 45), ("a", 3, 7, 99)], schema)
    with pytest.raises(Exception, match="overlap"):
        with run(spark, model.TemporalOrders, mode, [("a", 4, 1)], prices=prices) as result:
            result.collect()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_hook_schema(spark, mode):
    with pytest.raises(Exception, match="(?s)remove_amount.*amount"):
        with run(spark, model.BrokenHook, mode, [("a", 1, 2)]) as result:
            result.collect()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_hook_contract_with_intermediate_checks_disabled(spark, mode):
    with pytest.raises(ValueError, match="Hook remove_amount, relation orders.*amount"):
        with run(spark, model.BrokenHook, mode, [("a", 1, 2)], options={"validate_intermediate": False}):
            pytest.fail("hook-local schema contract was disabled")


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize(
    "transform", [model.UniqueOrders, model.PositiveOrders, model.LatestOrders, model.EarliestOrders]
)
@pytest.mark.parametrize("rows", [[], [("a", 1, 2), ("b", 2, 3)]])
def test_valid_rows(spark, mode, transform, rows):
    with run(spark, transform, mode, rows) as result:
        assert sorted(tuple(row) for row in result.collect()) == rows


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize("transform", [model.WrongTypeHook, model.WrongReturnHook, model.StrictExtraHook])
def test_invalid_hook_return(spark, mode, transform):
    with pytest.raises((ValueError, TypeError), match="Hook remove_amount, relation orders"):
        with run(spark, transform, mode, [("a", 1, 2)]):
            pytest.fail("invalid hook return accepted")


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_hook_projection(spark, mode):
    with run(spark, model.ExtraHook, mode, [("a", 1, 2)]) as result:
        assert result.columns == ["id", "revision", "amount"]
        assert tuple(result.first()) == ("a", 1, 2)


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize("transform", [model.RollupOrders, model.CubeOrders, model.GroupingSetsOrders])
def test_subtotal_null(spark, mode, transform):
    with run(spark, transform, mode, [("a", 1, None), ("b", 2, 5)]) as result:
        assert {tuple(row) for row in result.collect()} == {
            (None, False, 0, 1),
            (5, False, 0, 2),
            (None, True, 1, 3),
        }


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_aggregate_selection_tie(spark, mode):
    with pytest.raises(Exception, match="tie"):
        with run(spark, model.FirstAmount, mode, [("a", 1, 2), ("a", 1, 3)]) as result:
            result.collect()


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize("transform", [model.AsOfOrders, model.ForwardOrders, model.NearestOrders, model.LookupOrders])
def test_lookup_tie(spark, mode, transform):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("start", T.LongType(), False),
            T.StructField("end", T.LongType(), True),
            T.StructField("price", T.LongType(), False),
        ]
    )
    prices = spark.createDataFrame([("a", 4, 5, 40), ("a", 4, 5, 45)], schema)
    with pytest.raises(Exception, match="JOIN-E0601"):
        with run(spark, transform, mode, [("a", 4, 1)], prices=prices) as result:
            result.collect()


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize(
    "transform,rows,code",
    [
        (model.OnlyOrder, [("a", 1, 1), ("b", 1, 1)], "REL-E0701"),
        (model.PositiveThenFilter, [("a", 1, -1), ("b", 1, 1)], "REL-E0703"),
        (model.PositiveOrders, [("a", 1, None)], "REL-E0703"),
        (model.EarliestOrders, [("a", 1, 1), ("a", 1, 2), ("a", 2, 3)], "tie"),
        (model.LatestOrders, [("a", 1, 1), ("a", 1, 1)], "tie"),
        (model.CheckHierarchy, [("a", "missing", 1)], "REL-E0706"),
        (model.PriorityOrders, [("a", 1, -1), ("b", 1, 1)], "REL-E0705"),
    ],
)
def test_boundary_rejection(spark, mode, transform, rows, code):
    with pytest.raises(Exception, match=code):
        with run(spark, transform, mode, rows) as result:
            result.count()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_lower_ties_are_not_winners(spark, mode):
    rows = [("a", 1, 1), ("a", 1, 2), ("a", 2, 3)]
    with run(spark, model.LatestOrders, mode, rows) as result:
        assert [tuple(row) for row in result.collect()] == [("a", 2, 3)]


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_caller_cache_ownership(spark, mode):
    unrelated = spark.range(5, 8).cache()
    try:
        unrelated.count()
        with run(spark, model.UniqueOrders, mode, [("a", 1, 1)]) as result:
            cached = result.cache()
            try:
                assert cached.count() == 1
                assert cached.is_cached
            finally:
                cached.unpersist(blocking=True)
            assert not cached.is_cached
        if not os.environ.get("STRUCTURE_SPARK_REMOTE"):
            with run(spark, ReleaseOrders, mode, [("a", 1, 1)]) as result:
                assert result.count() == 1
        assert unrelated.is_cached
        assert unrelated.count() == 3
    finally:
        unrelated.unpersist(blocking=True)


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_reference_nulls_and_duplicates(spark, mode):
    from pyspark.sql import types as T

    reference = spark.createDataFrame([("a",), ("a",)], T.StructType([T.StructField("id", T.StringType(), False)]))
    rows = [("a", 1, 1), ("a", 1, 1), (None, 2, 3)]
    with run(spark, model.NullableReference, mode, rows, references=reference) as result:
        assert sorted(result.collect(), key=lambda row: row.revision) == rows
    with pytest.raises(Exception, match="REL-E0704"):
        with run(spark, model.RejectNullReference, mode, rows, references=reference) as result:
            result.collect()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_selected_null_order(spark, mode):
    with run(spark, model.NullableLatest, mode, [(None, None, 1), (None, 1, 2)]) as result:
        assert [tuple(row) for row in result.collect()] == [(None, 1, 2)]
    with pytest.raises(Exception, match="tie"):
        with run(spark, model.NullableLatest, mode, [(None, None, 1), (None, None, 2)]) as result:
            result.collect()


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_hook_return_arity(spark, mode):
    with pytest.raises(TypeError, match="Hook polish must return 2 DataFrames"):
        with run(spark, model.MultipleHook, mode, [("a", 1, 2)]):
            pytest.fail("wrong tuple arity accepted")


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_temporal_endpoints_and_left_duplicates(spark, mode):
    from pyspark.sql import types as T

    prices = spark.createDataFrame(
        [("a", 1, 5, 40), ("a", 5, None, 45)],
        PySpark.schema.materialize()(model.Price, types=T),
    )
    rows = [("a", 1, 1), ("a", 5, 2), ("a", 5, 2), ("b", 5, 3)]
    with run(spark, model.TemporalOrders, mode, rows, prices=prices) as result:
        assert sorted(tuple(row) for row in result.collect()) == [
            ("a", 1, 1, 40),
            ("a", 5, 2, 45),
            ("a", 5, 2, 45),
            ("b", 5, 3, None),
        ]


@pytest.mark.parametrize("mode", ["online", "generated"])
@pytest.mark.parametrize("transform", [model.UniqueOrders, model.PositiveOrders, model.LatestOrders])
def test_lazy_construction_and_eliminated_work(spark, mode, transform):
    if os.environ.get("STRUCTURE_SPARK_REMOTE"):
        from pyspark.sql.connect.dataframe import DataFrame

        with patch.object(DataFrame, "collect", side_effect=AssertionError("eager collect")), patch.object(
            DataFrame, "count", side_effect=AssertionError("eager count")
        ), patch.object(DataFrame, "first", side_effect=AssertionError("eager first")):
            with run(spark, transform, mode, [("a", 1, -1), ("a", 1, 2)]):
                pass
        return
    group = f"lazy-{mode}-{transform.__name__}"
    spark.sparkContext.setJobGroup(group, "construct guarded result")
    try:
        with run(spark, transform, mode, [("a", 1, -1), ("a", 1, 2)]) as result:
            assert spark.sparkContext.statusTracker().getJobIdsForGroup(group) == []
            assert result.limit(0).collect() == []
            assert result.where("false").count() == 0
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)


@pytest.mark.parametrize("mode", ["online", "generated"])
def test_assertion_retains_validation_input(spark, mode):
    rows = [("a", 1, -1), ("a", 1, 2), ("b", 1, 3)]
    with run(spark, model.UniqueOrders, mode, rows) as result:
        with pytest.raises(Exception, match="REL-E0702"):
            result.where("id = 'b'").collect()
    with run(spark, model.LatestOrders, mode, rows) as result:
        assert [tuple(row) for row in result.where("id = 'b'").collect()] == [("b", 1, 3)]
