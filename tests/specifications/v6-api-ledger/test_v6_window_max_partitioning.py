from typing import cast

from structure import Schema, Transform, input, output
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    double,
    rows_between,
    string,
    unbounded_following,
    unbounded_preceding,
    window,
    window_max,
)
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class QueryTermScore(Schema):
    query_id = string(nullable=False)
    term = string(nullable=False)
    normalized_score = double(nullable=False)


class QueryTermMaximum(Schema):
    query_id = string(nullable=False)
    term = string(nullable=False)
    partition_max_score = double(nullable=False)


class QueryPartitionMaximums(Transform):
    terms = input(QueryTermScore)
    maximums = output(QueryTermMaximum)

    def score(self, term: QueryTermScore) -> QueryTermMaximum:
        query_terms = window(
            partition_by=term.query_id,
            order_by=term.term,
            frame=rows_between(unbounded_preceding(), unbounded_following()),
        )
        return QueryTermMaximum(
            query_id=term.query_id,
            term=term.term,
            partition_max_score=window_max(term.normalized_score, over=query_terms),
        )


def test_window_max_keeps_query_partitions_isolated_in_generated_source() -> None:
    lowered = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(QueryPartitionMaximums, materialize_schemas=False).lowered,
    )

    text = render_pyspark_step(lowered.steps[0], current="terms", sources={"terms": "terms"})

    assert (
        'F.max(F.col("query_term_score.normalized_score")).over('
        'Window.partitionBy(F.col("query_term_score.query_id")).'
        'orderBy(F.col("query_term_score.term").asc()).'
        "rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))"
    ) in text
