"""Spark-free Delta liquid-clustering authoring and dispatch rules."""

import ast
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from structure import Schema, Transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import delta_optimize, delta_table, string
from structure.plugin.pyspark.delta.operations import DeltaOptimize, DeltaScope
from structure.plugin.pyspark.delta.runtime import execute_delta_optimize
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module


class Order(Schema):
    id = string()


def _compile(subject):
    return Compiler.frontend.compile()(subject, materialize_schemas=False, plugin={"pyspark": {}}).lowered


def test_full_reclustering_is_a_single_compiler_visible_effect() -> None:
    class Maintain(Transform):
        orders = delta_table(Order)

        def recluster(self, order: Order) -> None:
            delta_optimize(order).full()

    plan = _compile(Maintain)
    mutation = plan.steps[0].delta_mutations[0]
    assert mutation.kind == "optimize"
    assert mutation.action == "full"
    assert plan.steps[0].effect
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.Maintain",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    assert "execute_delta_optimize(" in source
    ast.parse(source)


def test_full_rejects_predicate_and_read_only_target() -> None:
    target = DeltaScope(name="orders", schema=Order, source="orders", binding="delta_table")
    with pytest.raises(TypeError, match="does not support where="):
        delta_optimize(target, where=True).full()
    readonly = DeltaScope(name="orders", schema=Order, source="orders", binding="delta_input")
    with pytest.raises(TypeError, match=r"requires a delta_table\(\.\.\.\) relation"):
        delta_optimize(readonly)


@pytest.mark.parametrize(
    "details",
    [
        {"tableFeatures": ["clustering"], "clusteringColumns": []},
        {"tableFeatures": ["Clustering"], "clusteringColumns": ["id"]},
    ],
)
@pytest.mark.parametrize("action,predicate", [("compaction", "`id` = '1'"), ("zorder", None)])
def test_clustered_tables_reject_predicates_and_zorder(details, action, predicate) -> None:
    table = Mock()
    table.detail.return_value.first.return_value.asDict.return_value = details
    with pytest.raises(ValueError, match="[Ll]iquid-clustered Delta tables"):
        execute_delta_optimize(table, predicate, action, ("id",), ("id",) if predicate else ())
    table.optimize.assert_not_called()


@pytest.mark.parametrize(
    "details",
    [
        {"tableFeatures": [], "clusteringColumns": []},
        {"tableFeatures": ["clustering"], "clusteringColumns": []},
    ],
)
def test_full_requires_active_clustering_keys(details) -> None:
    table = Mock()
    table.detail.return_value.first.return_value.asDict.return_value = details
    with pytest.raises(ValueError, match="requires active clustering keys"):
        execute_delta_optimize(table, None, "full", ())
    table.toDF.assert_not_called()


def test_full_uses_escaped_metadata_location_and_runs_command() -> None:
    table = Mock()
    table.detail.return_value.first.return_value.asDict.return_value = {
        "location": "file:/tmp/table`name",
        "tableFeatures": ["clustering"],
        "clusteringColumns": ["id"],
    }
    spark = table.toDF.return_value.sparkSession
    assert execute_delta_optimize(table, None, "full", ()) is None
    spark.sql.assert_called_once_with("OPTIMIZE delta.`file:/tmp/table``name` FULL")
    spark.sql.return_value.collect.assert_called_once_with()


def test_full_builder_cannot_execute_twice(monkeypatch) -> None:
    context = SimpleNamespace(delta_mutations=[])
    monkeypatch.setattr("structure.plugin.pyspark.delta.operations.current_pyspark_context", lambda: context)
    target = DeltaScope(name="orders", schema=Order, source="orders", binding="delta_table")
    builder = DeltaOptimize(target, where=None)
    builder.full()
    with pytest.raises(TypeError, match="executed only once"):
        builder.full()
    assert len(context.delta_mutations) == 1
