"""Spark-free contract tests for caller-bound Delta transform mutations."""

import ast

import pytest

from structure import Schema, Transform, input, output, step, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    check,
    delta_delete,
    delta_input,
    delta_merge,
    delta_output,
    delta_update,
    integer,
    string,
)
from structure.plugin.pyspark.delta.checks import bind_checks
from structure.plugin.pyspark.delta.runtime import _check_tree, _sql_tree
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)
    constraints = (check(status != "invalid", name="valid_status"),)


class Change(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


def _compile(subject, **plugin):
    return Compiler.frontend.compile()(subject, materialize_schemas=False, plugin={"pyspark": plugin})


def test_delta_output_is_caller_bound_and_delete_effect_is_retained() -> None:
    @transform
    class Delete(Transform):
        orders = delta_output(Order)

        @step(input=orders, output=orders)
        def delete(self, order: Order) -> None:
            delta_delete(order, where=order.id == 1)

    handle = object()
    assert Delete(orders=handle)._structure_bound_inputs["orders"] is handle
    plan = _compile(Delete).lowered
    assert plan.inputs[0].binding == "delta"
    assert plan.steps[0].effect
    assert plan.steps[0].delta_mutations[0].kind == "delete"
    assert tuple(step.name for step in plan.steps) == ("delete",)
    assert plan.outputs[0].binding == "delta"


def test_delta_input_cannot_be_mutated() -> None:
    @transform
    class Invalid(Transform):
        orders = delta_input(Order)
        result = delta_output(Order)

        @step(input=orders, output=result)
        def mutate(self, order: Order) -> None:
            delta_delete(order, where=True)

    with pytest.raises(Exception, match="delta_output"):
        _compile(Invalid)


def test_update_requires_typed_assignment() -> None:
    @transform
    class Update(Transform):
        orders = delta_output(Order)

        @step(input=orders, output=orders)
        def update(self, order: Order) -> None:
            delta_update(order, where=order.id == 1, set=Order(status="done"))

    mutation = _compile(Update).lowered.steps[0].delta_mutations[0]
    assert mutation.assignments[0][0] == "status"
    assert mutation.assignments[0][1].data["value"] == "done"


def test_merge_compiles_matched_unmatched_and_source_clauses() -> None:
    @transform
    class Sync(Transform):
        changes = input(Change)
        orders = delta_output(Order)

        @step(input=(changes, orders), output=orders)
        def sync(self, change: Change, order: Order) -> None:
            (
                delta_merge(order, change, on=order.id == change.id)
                .when_matched_update(set=Order(status=change.status))
                .when_not_matched_insert(values=Order(id=change.id, status=change.status))
                .when_not_matched_by_source_delete(condition=order.status == "expired")
                .execute()
            )

    mutation = _compile(Sync).lowered.steps[0].delta_mutations[0]
    assert mutation.source == "changes"
    assert tuple(clause.action for clause in mutation.clauses) == (
        "matched_update",
        "unmatched_insert",
        "source_delete",
    )


def test_check_comparison_ignores_cosmetic_sql_changes() -> None:
    bound = bind_checks(Order)[0]
    assert _check_tree(bound.predicate, False) == _sql_tree(" ((STATUS <> 'invalid')) ", False)
    assert _check_tree(bound.predicate, False) != _sql_tree("status <> 'other'", False)
    with pytest.raises(ValueError, match="Unsupported"):
        _sql_tree("status RLIKE 'x'", False)


def test_generated_code_preserves_table_handle() -> None:
    @transform
    class Delete(Transform):
        orders = delta_output(Order)

        @step(input=orders, output=orders)
        def delete(self, order: Order) -> None:
            delta_delete(order, where=True)

    plan = _compile(Delete).lowered
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.Delete",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    ast.parse(source)
    assert "validate_delta_table(orders, _StructureDeltaSchema_0" in source
    assert "self._delta_tables['orders']" in source
    assert ".delete(F.lit(True))" in source


def test_delta_input_is_a_readable_relation_for_ordinary_steps() -> None:
    class IdOnly(Schema):
        id = integer(nullable=False)

    @transform
    class Read(Transform):
        orders = delta_input(Order)
        ids = output(IdOnly)

        @step(input=orders, output=ids)
        def select(self, order: Order) -> IdOnly:
            return IdOnly(id=order.id)

    plan = _compile(Read).lowered
    assert plan.inputs[0].binding == "delta"
    assert not plan.steps[0].effect
    assert plan.outputs[0].binding == "dataframe"


def test_delta_check_match_precedence_is_recorded_in_plan() -> None:
    @transform(delta_check_match="name")
    class Delete(Transform):
        orders = delta_output(Order)

        @step(input=orders, output=orders, delta_check_match="off")
        def delete(self, order: Order) -> None:
            delta_delete(order, where=True)

    plan = _compile(Delete, delta_check_match="expression").lowered
    assert plan.delta_check_match == "name"
    assert plan.steps[0].delta_check_match == "off"
    with pytest.raises(ValueError, match="delta_check_match"):
        _compile(Delete, delta_check_match="invalid")


def test_multiple_delta_effect_steps_keep_source_order() -> None:
    @transform
    class ChangeTwice(Transform):
        orders = delta_output(Order)

        @step(input=orders, output=orders)
        def delete(self, order: Order) -> None:
            delta_delete(order, where=order.id == 1)

        @step(input=orders, output=orders)
        def update(self, order: Order) -> None:
            delta_update(order, where=True, set=Order(status="done"))

    plan = _compile(ChangeTwice).lowered
    assert tuple(step.name for step in plan.steps) == ("delete", "update")


def test_unmatched_by_source_cannot_read_merge_source() -> None:
    @transform
    class Invalid(Transform):
        changes = input(Change)
        orders = delta_output(Order)

        @step(input=(changes, orders), output=orders)
        def merge(self, change: Change, order: Order) -> None:
            (delta_merge(order, change, on=order.id == change.id)
             .when_not_matched_by_source_update(set=Order(status=change.status))
             .execute())

    with pytest.raises(Exception, match="unavailable relation scope"):
        _compile(Invalid)
