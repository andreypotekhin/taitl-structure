import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session

from structure import Schema, Transform, input, lane, output, step
from structure.plugin.pyspark import integer, param_join, where
from structure.plugin.pyspark.execution.logic.PolicyChecks import singleton_policy
from structure.plugin.pyspark.execution.logic.running.RunOnlinePySparkTransform import RunOnlinePySparkTransform

pytestmark = pytest.mark.integration
MODULE = "integration.pyspark.support.test_policy_checks"
PACKAGE = "integration_policy_checks_generated"


class Event(Schema):
    id = integer(nullable=False)


class Policy(Schema):
    value = integer(nullable=True)


class Scored(Schema):
    id = integer(nullable=False)
    value = integer(nullable=True)


class UsePolicyTwice(Transform):
    events = input(Event)
    policy = input(Policy)
    scored = lane(Scored)
    result = output(Scored)

    @step(input=[events, policy], output=scored)
    def score(self, event: Event, policy: Policy) -> Scored:
        param_join(policy)
        return Scored(id=event.id, value=policy.value)

    @step(input=[scored, policy], output=result)
    def publish(self, scored: Scored, policy: Policy) -> Scored:
        param_join(policy)
        where(policy.value.is_null() | (policy.value >= 0))
        return scored


def test_policy_checks_leave_streaming_frames_unchanged(spark):
    stream = spark.readStream.format("rate").load()
    assert singleton_policy(stream, "policy") is stream


@pytest.mark.parametrize("values", [[], [None], [4], [-1, 4], [4, 4], list(range(20))])
def test_policy_guard_preserves_schema_and_cardinality_failures(spark, values):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    schema = T.StructType([T.StructField("value", T.IntegerType(), True, {"note": "preserved"})])
    source = spark.createDataFrame([(value,) for value in values], schema)
    old = RunOnlinePySparkTransform()._exactly_one(source, "policy", functions=F)
    checked = singleton_policy(source, "policy")
    assert checked.schema == source.schema
    # Connect can prune the old empty cross join, including its guard. The bounded check must still reject zero rows.
    for frame in ((checked,) if not values else (old, checked)):
        if len(values) != 1:
            with pytest.raises(Exception, match="REL-E0701"):
                frame.where(F.col("value") >= 0).collect()
        else:
            assert frame.collect() == source.collect()


@pytest.mark.parametrize("values", [[], [None], [4], [-1, 4]])
def test_policy_checks_online_generated_parity(spark, tmp_path, values):
    files = render_generated_project(
        UsePolicyTwice,
        source_transform=f"{MODULE}.UsePolicyTwice",
        generated_package=PACKAGE,
        source_schema_modules={MODULE: [Event, Policy, Scored]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_policy_checks")
        events = spark.createDataFrame([(1,), (2,)], schemas.EVENT_SCHEMA)
        policy = spark.createDataFrame([(value,) for value in values], schemas.POLICY_SCHEMA)
        results = []
        for mode in ("online", "generated"):
            result = UsePolicyTwice(events=events, policy=policy).run(
                session(spark, execution_mode=mode, generated_package=PACKAGE)
            ).result
            if len(values) != 1:
                with pytest.raises(Exception, match="REL-E0701"):
                    result.collect()
            else:
                assert result.schema == schemas.SCORED_SCHEMA
                results.append(result.orderBy("id").collect())
        if results:
            assert results[0] == results[1]
            assert len(results[0]) == 2
