from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.core.target.capabilities.api import BackendCapabilityError
from structure.plugin.pyspark import cross_join, exactly_one, integer, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class Event(Schema):
    event_id = string(nullable=False)


class Policy(Schema):
    max_ratio = integer(nullable=False)


class Scored(Schema):
    event_id = string(nullable=False)
    max_ratio = integer(nullable=False)


class AssertedPolicyTransform(Transform):
    events = input(Event)
    policy = input(Policy)
    scored = output(Scored)

    def score(self, event: Event, policy: Policy) -> Scored:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        return Scored(event_id=event.event_id, max_ratio=policy.max_ratio)


def test_exactly_one_records_a_relation_cardinality_operation() -> None:
    lowered = _lowered()
    operations = lowered.steps[0].operations

    assert [operation.kind for operation in operations] == ["exactly_one", "join"]
    assert operations[0].exactly_one is not None
    assert operations[0].exactly_one.scope == "policy"


def test_exactly_one_prepares_join_source_with_spark_visible_assertion() -> None:
    text = render_pyspark_step(
        _lowered().steps[0],
        current="events",
        sources={"events": "events", "policy": "policy"},
    )

    assert 'events_policy_exactly_one_1_count = policy.agg(F.count(F.lit(1)).alias("__structure_count"))' in text
    assert "F.assert_true(F.col(\"__structure_count\") == F.lit(1), 'REL-E0701:" in text
    assert (
        'events_policy_exactly_one_1 = events_policy_exactly_one_1_count.crossJoin(policy).drop("__structure_exactly_one")'
        in text
    )
    assert 'policy_joined = events_policy_exactly_one_1.alias("policy")' in text
    assert text.index("events_policy_exactly_one_1_count =") < text.index("events = events.crossJoin(policy_joined)")


def test_exactly_one_is_ordinary_pyspark_only() -> None:
    with pytest.raises(BackendCapabilityError):
        Compiler.frontend.compile()(
            AssertedPolicyTransform,
            materialize_schemas=False,
            plugin={"pyspark": {"variant": "spark-connect"}},
        )


def test_exactly_one_explain_names_scope_and_streaming_status() -> None:
    text = render_explain_report(AssertedPolicyTransform)

    assert "operations: exactly_one(row_preserving scope=policy), rowset_join(row_multiplying)" in text
    assert "status: compatible" in text


def test_exactly_one_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.AssertedPolicyTransform",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["score.exactly_one[0].policy"]
    assert dependency.sources == ("policy",)
    assert dependency.operation == "exactly_one"
    assert dependency.detail["diagnostic"] == "REL-E0701"


def test_exactly_one_rejects_non_relation_values() -> None:
    class BadTransform(Transform):
        events = input(Event)
        scored = output(Scored)

        def score(self, event: Event) -> Scored:
            exactly_one(cast(Policy, object()))
            return Scored(event_id=event.event_id, max_ratio=0)

    with pytest.raises(TypeError, match="exactly_one\\(relation\\) requires a Structure relation"):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


def test_exactly_one_rejects_already_joined_relation() -> None:
    class LateAssertionTransform(Transform):
        events = input(Event)
        policy = input(Policy)
        scored = output(Scored)

        def score(self, event: Event, policy: Policy) -> Scored:
            cross_join(policy, allow_cartesian=True)
            exactly_one(policy)
            return Scored(event_id=event.event_id, max_ratio=policy.max_ratio)

    with pytest.raises(TypeError, match="exactly_one\\(relation\\) must be called before that relation is joined"):
        Compiler.frontend.compile()(LateAssertionTransform, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(AssertedPolicyTransform, materialize_schemas=False).lowered,
    )
