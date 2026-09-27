import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import pytest

from structure import Schema, Transform, input, output
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import PySpark, integer, param_join
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.execution.logic.PolicyChecks import reuse_policy_checks, singleton_policy

MODULE = "structure.plugin.pyspark.execution.logic.PolicyChecks"


class Checked:
    def select(self, expression):
        assert expression == "__structure_policy.*"
        return self


class Policy(Schema):
    value = integer(nullable=False)


class ApplyPolicy(Transform):
    rows = input(Policy)
    policy = input(Policy)
    result = output(Policy)

    def apply(self, row: Policy, policy: Policy) -> Policy:
        param_join(policy)
        return row


class DerivedPolicy(ApplyPolicy):
    pass


@pytest.mark.parametrize("transform", [ApplyPolicy, DerivedPolicy])
@pytest.mark.parametrize("options", [(), ("mirror_methods",)])
@pytest.mark.parametrize("operations_only", [False, True])
def test_all_generated_layouts_scope_policy_reuse(transform, options, operations_only):
    plan = cast(PySparkExecutionPlan, Compiler.frontend.compile()(transform, materialize_schemas=False).lowered)
    if operations_only:
        plan = replace(plan, steps=tuple(replace(step, joins=()) for step in plan.steps))
    text = PySpark.render.transform()(
        plan,
        source_transform=f"{transform.__module__}.{transform.__name__}",
        runtime_module="generated.runtime",
        schema_modules={Policy: "generated.schemas"},
        generated_code_options=options,
    )
    ast.parse(text)
    assert "    @reuse_policy_checks\n    def run(" in text
    assert "singleton_policy(" in text


def test_policy_checks_reuse_only_identical_frames_and_scopes_within_one_run():
    frame = SimpleNamespace(isStreaming=False)
    other = SimpleNamespace(isStreaming=False)

    @reuse_policy_checks
    def run():
        first = singleton_policy(frame, "policy")
        assert singleton_policy(frame, "policy") is first
        assert singleton_policy(other, "policy") is not first
        assert singleton_policy(frame, "other") is not first
        return first

    with patch(f"{MODULE}.check_policy", side_effect=lambda *_: Checked()) as check:
        assert run() is not run()
        assert check.call_count == 6


def test_failed_and_nested_runs_release_their_checks():
    frame = SimpleNamespace(isStreaming=False)

    @reuse_policy_checks
    def inner():
        singleton_policy(frame, "policy")
        raise ValueError("stop")

    @reuse_policy_checks
    def outer():
        first = singleton_policy(frame, "policy")
        with pytest.raises(ValueError, match="stop"):
            inner()
        assert singleton_policy(frame, "policy") is first

    with patch(f"{MODULE}.check_policy", side_effect=lambda *_: Checked()) as check:
        outer()
        singleton_policy(frame, "policy")
        singleton_policy(frame, "policy")
        assert check.call_count == 4


def test_concurrent_runs_do_not_share_policy_checks():
    frame = SimpleNamespace(isStreaming=False)
    barrier = Barrier(2)

    @reuse_policy_checks
    def run():
        first = singleton_policy(frame, "policy")
        barrier.wait(timeout=5)
        assert singleton_policy(frame, "policy") is first
        return first

    with patch(f"{MODULE}.check_policy", side_effect=lambda *_: Checked()) as check:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: run(), range(2)))
        assert results[0] is not results[1]
        assert check.call_count == 2


def test_streaming_policy_bypasses_checks():
    frame = SimpleNamespace(isStreaming=True)
    with patch(f"{MODULE}.check_policy") as check:
        assert singleton_policy(frame, "policy") is frame
        check.assert_not_called()
