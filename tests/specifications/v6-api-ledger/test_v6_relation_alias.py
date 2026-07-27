from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import double, inner_join, relation_alias, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class DirectedScore(Schema):
    left_id = string(nullable=False)
    right_id = string(nullable=False)
    score = double(nullable=False)


class ReciprocalScore(Schema):
    left_id = string(nullable=False)
    right_id = string(nullable=False)
    left_score = double(nullable=False)
    right_score = double(nullable=False)


class MatchReciprocalScores(Transform):
    scores = input(DirectedScore)
    reciprocals = output(ReciprocalScore)

    def match(self, score: DirectedScore) -> ReciprocalScore:
        reversed_score = relation_alias(score, name="reversed_score")
        joined = inner_join(
            reversed_score,
            on=(score.left_id == reversed_score.right_id) & (score.right_id == reversed_score.left_id),
        )
        return ReciprocalScore(
            left_id=score.left_id,
            right_id=score.right_id,
            left_score=score.score,
            right_score=joined.score,
        )


def test_relation_alias_records_named_self_scope_before_join() -> None:
    operations = _lowered().steps[0].operations

    assert operations[0].kind == "relation_alias"
    assert operations[0].relation_alias is not None
    assert operations[0].relation_alias.alias == "reversed_score"
    assert operations[0].relation_alias.source == "scores"
    assert operations[1].kind == "join"
    assert operations[1].join is not None
    assert operations[1].join.input_name == "reversed_score"
    assert operations[1].join.source == "scores"


def test_relation_alias_renders_self_join_against_same_source_frame() -> None:
    text = render_pyspark_step(_lowered().steps[0], current="scores", sources={"scores": "scores"})

    assert 'scores = scores.alias("directed_score")' in text
    assert 'reversed_score_joined = scores.alias("reversed_score")' in text
    assert 'F.col("directed_score.left_id") == F.col("reversed_score.right_id")' in text
    assert 'F.col("reversed_score.score").alias("right_score")' in text


def test_relation_alias_explain_and_traceability_are_compiler_visible() -> None:
    text = render_explain_report(MatchReciprocalScores)
    assert "relation_alias(row_preserving source=scores alias=reversed_score schema=DirectedScore)" in text

    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.MatchReciprocalScores",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["match.relation_alias[0].reversed_score"]
    assert dependency.sources == ("scores",)
    assert dependency.operation == "relation_alias"


def test_relation_alias_rejects_reads_before_join() -> None:
    class BadRead(Transform):
        scores = input(DirectedScore)
        reciprocals = output(ReciprocalScore)

        def match(self, score: DirectedScore) -> ReciprocalScore:
            reversed_score = relation_alias(score, name="reversed_score")
            return ReciprocalScore(
                left_id=score.left_id,
                right_id=reversed_score.right_id,
                left_score=score.score,
                right_score=reversed_score.score,
            )

    with pytest.raises(Exception, match="reads relation parameter reversed_score before it is joined"):
        Compiler.frontend.compile()(BadRead, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(MatchReciprocalScores, materialize_schemas=False).lowered,
    )
