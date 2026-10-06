"""Spark-free contract tests for caller-bound Delta transform mutations."""

import ast

import pytest

from structure import Schema, Transform, input, output, step, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    check,
    delta_append,
    delta_changes,
    delta_default,
    delta_delete,
    delta_detail,
    delta_generated,
    delta_history,
    delta_identity,
    delta_input,
    delta_merge,
    delta_optimize,
    delta_output,
    delta_replace_where,
    delta_restore,
    delta_snapshot,
    delta_table,
    delta_update,
    delta_vacuum,
    integer,
    long,
    string,
)
from structure.plugin.pyspark.delta.checks import bind_checks
from structure.plugin.pyspark.delta.operations import DeltaScope, _assignments
from structure.plugin.pyspark.delta.runtime import _check_tree, _compact_sql, _sql_tree
from structure.plugin.pyspark.delta.schema import resolve_delta_columns
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)
    constraints = (check(status != "invalid", name="valid_status"),)


class Change(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


class ChangeV2(Change):
    note = string()


class OrderV2(Order):
    note = string()


class GeneratedOrder(Schema):
    base = integer(nullable=False)
    derived = integer()
    delta_columns = (delta_generated(derived, as_="base + 1"),)


class IdentityOrder(Schema):
    id = long()
    status = string(nullable=False)
    delta_columns = (delta_identity(id),)


class DefaultOrder(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)
    delta_columns = (delta_default(status, value="open"),)


def _compile(subject, **plugin):
    return Compiler.frontend.compile()(subject, materialize_schemas=False, plugin={"pyspark": plugin})


def test_delta_table_is_caller_bound_and_delete_effect_is_retained() -> None:
    @transform
    class Delete(Transform):
        orders = delta_table(Order)

        def delete(self, order: Order) -> None:
            delta_delete(order, where=order.id == 1)

    handle = object()
    assert Delete(orders=handle)._structure_bound_inputs["orders"] is handle
    plan = _compile(Delete).lowered
    assert plan.inputs[0].binding == "delta_table"
    assert plan.steps[0].effect
    assert plan.steps[0].delta_mutations[0].kind == "delete"
    assert tuple(step.name for step in plan.steps) == ("delete",)
    assert plan.outputs[0].binding == "delta_table"


@pytest.mark.parametrize(
    ("profile", "capability"),
    [
        (">=3.5,<4.1", "binding"),
        (">=3.5,<4.0", "merge"),
        (">=4.0,<4.1", "changes"),
        (">=4.1,<4.2", "vacuum"),
    ],
)
def test_delta_capabilities_are_available_on_admitted_classic_profiles(profile, capability) -> None:
    from structure.plugin.api.v1.model import CapabilityRequirement
    from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities

    assert PySparkCapabilities(target_profile=profile).supports(
        CapabilityRequirement(group="delta", name=capability)
    ).supported


def test_delta_capability_is_not_claimed_for_spark_connect() -> None:
    from structure.plugin.api.v1.model import CapabilityRequirement
    from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities

    decision = PySparkCapabilities(
        target_profile=">=4.0,<4.1", target_variant="spark-connect"
    ).supports(CapabilityRequirement(group="delta", name="binding"))
    assert not decision.supported
    assert "ordinary PySpark" in decision.use


@pytest.mark.parametrize("profile", [">=3.5,<4.1", ">=3.5,<4.0", ">=4.0,<4.1", ">=4.1,<4.2"])
def test_delta_spark_connect_profiles_fail_before_plan_lowering(profile) -> None:
    from structure.plugin.api.v1.model import BackendCapabilityError

    @transform
    class Delete(Transform):
        orders = delta_table(Order)

        def delete(self, order: Order) -> None:
            delta_delete(order, where=True)

    with pytest.raises(BackendCapabilityError) as raised:
        _compile(Delete, profile=profile, variant="spark-connect")
    assert "delta.binding" in str(raised.value)
    if profile == ">=4.1,<4.2":
        assert "ordinary PySpark" in raised.value.diagnostic.use


@pytest.mark.parametrize(
    ("spark_version", "delta_version"),
    [("3.5.0", "4.1.0"), ("3.5.3", "3.3.3"), ("4.0.0", "4.0.1"), ("4.1.0", "4.1.0")],
)
def test_delta_runtime_pair_check(monkeypatch, spark_version, delta_version) -> None:
    from structure.plugin.pyspark.delta import runtime

    class Session:
        version = spark_version

    class Frame:
        sparkSession = Session()

    class Table:
        def toDF(self):
            return Frame()

    monkeypatch.setattr(runtime, "version", lambda _: delta_version)
    if (spark_version, delta_version) == ("3.5.0", "4.1.0"):
        with pytest.raises(RuntimeError, match="Unsupported Spark/Delta runtime pair"):
            runtime.require_compatible_delta_runtime(Table())
    else:
        runtime.require_compatible_delta_runtime(Table())


def test_delta_column_metadata_only_relaxes_declared_insert_fields() -> None:
    generated = DeltaScope(name="generated", schema=GeneratedOrder, source="generated", binding="delta_table")
    identity = DeltaScope(name="identity", schema=IdentityOrder, source="identity", binding="delta_table")
    default = DeltaScope(name="default", schema=DefaultOrder, source="default", binding="delta_table")

    assert resolve_delta_columns(GeneratedOrder)["derived"].kind == "generated"
    assert _assignments(generated, GeneratedOrder(base=2), insert=True)[0][0] == "base"
    assert _assignments(identity, IdentityOrder(status="open"), insert=True)[0][0] == "status"
    assert _assignments(default, DefaultOrder(id=1), insert=True)[0][0] == "id"
    with pytest.raises(TypeError, match="GENERATED ALWAYS"):
        _assignments(identity, IdentityOrder(id=7, status="open"), insert=True)
    with pytest.raises(TypeError, match="GENERATED ALWAYS"):
        _assignments(identity, IdentityOrder(id=7, status="open"))


def test_delta_column_metadata_rejects_invalid_declarations() -> None:
    with pytest.raises(ValueError, match="mode"):
        delta_identity("id", mode="sometimes")
    with pytest.raises(ValueError, match="step"):
        delta_identity("id", step=0)
    with pytest.raises(TypeError, match="SQL expression"):
        delta_generated("id", as_="")

    class ForeignField(Schema):
        id = integer()

    class Invalid(Schema):
        id = integer()
        delta_columns = (delta_default(ForeignField.id, value=1),)

    with pytest.raises(TypeError, match="outside that Schema"):
        resolve_delta_columns(Invalid)

    class InvalidIdentity(Schema):
        id = integer()
        delta_columns = (delta_identity(id),)

    with pytest.raises(TypeError, match="long type"):
        resolve_delta_columns(InvalidIdentity)


def test_generated_expression_comparison_preserves_quoted_whitespace() -> None:
    assert _compact_sql("base + 1") == _compact_sql("base+1")
    assert _compact_sql("concat(value, 'a b')") != _compact_sql("concat(value, 'ab')")


def test_same_schema_merge_can_return_a_typed_delta_result() -> None:
    @transform
    class Merge(Transform):
        changes = input(Change)
        orders = delta_table(Order)

        def merge(self, change: Change, order: Order) -> Order:
            return (
                delta_merge(order, change, on=order.id == change.id)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

    step_plan = _compile(Merge).lowered.steps[0]
    assert step_plan.effect
    assert step_plan.output_schema is Order
    assert step_plan.delta_mutations[0].output_schema is Order


def test_typed_same_schema_merge_rejects_an_incompatible_return_schema() -> None:
    @transform
    class Invalid(Transform):
        changes = input(Change)
        orders = delta_table(Order)

        def merge(self, change: Change, order: Order) -> OrderV2:
            return delta_merge(order, change, on=order.id == change.id).execute()

    with pytest.raises(Exception, match="Cannot deduce final output orders"):
        _compile(Invalid)


def test_delta_table_can_be_resolved_through_step_inout() -> None:
    @transform
    class Delete(Transform):
        changes = input(Change)
        orders = delta_table(Order)

        @step(inout=(changes, orders) | orders)
        def delete(self, change: Change, order: Order) -> None:
            delta_delete(order, where=order.id == 1)

    plan = _compile(Delete).lowered
    step_plan = plan.steps[0]
    assert step_plan.effect
    assert {binding.binding for binding in plan.inputs} == {"dataframe", "delta_table"}


def test_delta_input_cannot_be_mutated() -> None:
    @transform
    class Invalid(Transform):
        orders = delta_input(Order)
        result = delta_output(Order)

        @step(input=orders, output=result)
        def mutate(self, order: Order) -> None:
            delta_delete(order, where=True)

    with pytest.raises(Exception, match="delta_input|Delta table result"):
        _compile(Invalid)


def test_update_requires_typed_assignment() -> None:
    @transform
    class Update(Transform):
        orders = delta_table(Order)

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
        orders = delta_table(Order)

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
        orders = delta_table(Order)

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
    assert "validated_delta_frame(orders, _StructureDeltaSchema_0" in source
    assert "self._delta_tables['orders']" in source
    assert ".delete(F.lit(True))" in source


def test_replace_where_requires_execute_and_renders_safe_predicate() -> None:
    @transform
    class Replace(Transform):
        orders = delta_table(Order)
        replacements = input(Order)

        @step(input=(orders, replacements), output=orders)
        def replace(self, order: Order, replacement: Order) -> None:
            delta_replace_where(order, replacement, where=order.status == "ready").execute()

    plan = _compile(Replace).lowered
    mutation = plan.steps[0].delta_mutations[0]
    assert mutation.kind == "replace_where"
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.Replace",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    ast.parse(source)
    assert ".mode('overwrite').option('replaceWhere'" in source
    assert "`status` = 'ready'" in source


def test_delta_replace_where_sql_escapes_literals_and_binds_variables() -> None:
    from structure.plugin.pyspark.delta.runtime import bind_delta_predicate_variables

    assert bind_delta_predicate_variables("(`status` = {{status}})", {"status": "O'Reilly"}) == "(`status` = 'O''Reilly')"


def test_snapshot_and_cdf_reads_are_typed_non_effect_steps() -> None:
    from structure import variable
    from structure.plugin.pyspark import long

    class Change(Schema):
        id = integer(nullable=False)
        status = string(nullable=False)
        change_type = string(alias="_change_type")
        commit_version = long(alias="_commit_version")

    @transform
    class Reads(Transform):
        orders = delta_input(Order)
        version = variable(int)
        first_version = variable(int)
        changes = output(Change)

        @step(input=orders, output=changes)
        def changes_since(self, order: Order) -> Change:
            return delta_changes(order, starting_version=self.first_version)

    plan = _compile(Reads).lowered
    assert not plan.steps[0].effect
    assert plan.steps[0].delta_mutations[0].kind == "delta_changes"
    assert plan.steps[0].delta_mutations[0].output_schema is Change
    assert plan.steps[0].delta_mutations[0].selector.kind == "variable"
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.Reads",
        schema_modules={Order: "tests.schemas", Change: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    ast.parse(source)
    assert "open_delta_relation(" in source
    assert "'delta_changes'" in source


def test_history_and_detail_are_typed_non_effect_reads() -> None:
    from structure import variable
    from structure.plugin.pyspark import long

    class Commit(Schema):
        version = long(nullable=False)
        operation = string()

    class Detail(Schema):
        format = string(nullable=False)
        location = string(nullable=False)

    @transform
    class Reads(Transform):
        orders = delta_input(Order)
        limit = variable(int, default=2)
        commits = output(Commit)
        details = output(Detail)

        @step(input=orders, output=commits)
        def history(self, order: Order) -> Commit:
            return delta_history(order, limit=self.limit)

        @step(input=orders, output=details)
        def detail(self, order: Order) -> Detail:
            return delta_detail(order)

    plan = _compile(Reads).lowered
    history, detail = plan.steps
    assert not history.effect and not detail.effect
    assert history.delta_mutations[0].kind == "delta_history"
    assert history.delta_mutations[0].selector.kind == "variable"
    assert history.delta_mutations[0].output_schema is Commit
    assert detail.delta_mutations[0].kind == "delta_detail"
    assert detail.delta_mutations[0].output_schema is Detail
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.Reads",
        schema_modules={Order: "tests.schemas", Commit: "tests.schemas", Detail: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    ast.parse(source)
    assert "'delta_history'" in source
    assert "'delta_detail'" in source


@pytest.mark.parametrize("limit", [True, 0, -1, 1.5])
def test_delta_history_rejects_invalid_literal_limit(limit) -> None:
    class Commit(Schema):
        version = integer(nullable=False)

    @transform
    class Reads(Transform):
        orders = delta_input(Order)
        commits = output(Commit)

        @step(input=orders, output=commits)
        def history(self, order: Order) -> Commit:
            return delta_history(order, limit=limit)

    with pytest.raises(Exception, match="positive integer"):
        _compile(Reads)


def test_restore_has_a_typed_direct_table_result() -> None:
    from structure import variable

    @transform
    class Restore(Transform):
        current = delta_input(Order)
        restored = delta_output(OrderV2)
        version = variable(int)

        def apply(self, order: Order) -> OrderV2:
            return delta_restore(order, version=self.version).execute()

    mutation = _compile(Restore).lowered.steps[0].delta_mutations[0]
    assert mutation.kind == "restore"
    assert mutation.selector_type == "version"
    assert mutation.output_schema is OrderV2
    assert mutation.output == "restored"
    source = render_pyspark_transform_module(
        _compile(Restore).lowered,
        source_transform=f"{__name__}.Restore",
        schema_modules={Order: "tests.schemas", OrderV2: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    ast.parse(source)
    assert "execute_delta_restore(" in source


def test_optimize_and_vacuum_are_explicit_delta_table_effects() -> None:
    from structure import variable

    @transform
    class Maintain(Transform):
        orders = delta_table(Order)
        retention = variable(float, default=168.0)

        @step(inout=orders | orders)
        def maintain(self, order: Order) -> None:
            delta_optimize(order).execute_zorder(by=(order.id,))
            delta_vacuum(order, retention_hours=self.retention).execute()

    plan = _compile(Maintain).lowered
    mutations = plan.steps[0].delta_mutations
    assert [mutation.kind for mutation in mutations] == ["optimize", "vacuum"]
    assert mutations[0].action == "zorder"
    assert mutations[0].columns == ("id",)
    assert mutations[1].selector.kind == "variable"
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.Maintain",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    ast.parse(source)
    assert "execute_delta_optimize(" in source
    assert "execute_delta_vacuum(" in source


@pytest.mark.parametrize("retention", [-1, float("inf"), float("nan")])
def test_vacuum_rejects_invalid_retention_at_compile_time(retention) -> None:
    @transform
    class Vacuum(Transform):
        orders = delta_table(Order)

        @step(inout=orders | orders)
        def vacuum(self, order: Order) -> None:
            delta_vacuum(order, retention_hours=retention, allow_short_retention=True).execute()

    with pytest.raises(Exception, match="finite and nonnegative"):
        _compile(Vacuum)


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
        orders = delta_table(Order)

        @step(input=orders, output=orders, delta_check_match="off")
        def delete(self, order: Order) -> None:
            delta_delete(order, where=True)

    plan = _compile(Delete, delta_check_match="expression").lowered
    assert plan.delta_check_match == "name"
    assert plan.steps[0].delta_check_match == "off"
    with pytest.raises(ValueError, match="delta_check_match"):
        _compile(Delete, delta_check_match="invalid")


def test_delta_cdf_checks_follow_plugin_transform_and_step_precedence() -> None:
    @transform(delta_cdf_checks=False)
    class ReadChanges(Transform):
        orders = delta_input(Order)
        changes = output(Order)

        @step(input=orders, output=changes, delta_cdf_checks=True)
        def read(self, order: Order) -> Order:
            return delta_changes(order, starting_version=1)

    plan = _compile(ReadChanges, delta_cdf_checks=True).lowered
    assert not plan.delta_cdf_checks
    assert plan.steps[0].delta_cdf_checks is True
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.ReadChanges",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    assert "check_cdf_configuration=True" in source
    with pytest.raises(ValueError, match="delta_cdf_checks"):
        _compile(ReadChanges, delta_cdf_checks="off")

    @transform
    class PluginConfigured(Transform):
        orders = delta_input(Order)
        changes = output(Order)

        def read(self, order: Order) -> Order:
            return delta_changes(order, starting_version=1)

    configured_plan = _compile(PluginConfigured, delta_cdf_checks=False).lowered
    assert not configured_plan.delta_cdf_checks
    configured_source = render_pyspark_transform_module(
        configured_plan,
        source_transform=f"{__name__}.PluginConfigured",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    assert "check_cdf_configuration=False" in configured_source


def test_delta_cdf_session_validation_accepts_case_insensitive_delta_names() -> None:
    from structure.plugin.pyspark.delta.runtime import _validate_delta_cdf_configuration

    class Conf:
        values = {
            "spark.sql.extensions": "vendor.DELTA.Extension",
            "spark.sql.catalog.spark_catalog": "vendor.delta.Catalog",
        }

        def get(self, name, default=""):
            return self.values.get(name, default)

    class Spark:
        conf = Conf()

    _validate_delta_cdf_configuration({"delta.enablechangedatafeed": "true"}, Spark())
    with pytest.raises(ValueError, match="change feed is not enabled"):
        _validate_delta_cdf_configuration({}, Spark())
    Spark.conf.values["spark.sql.catalog.spark_catalog"] = "org.example.jdbc.Catalog"
    with pytest.raises(RuntimeError, match="Delta SQL extension and catalog"):
        _validate_delta_cdf_configuration({"delta.enablechangedatafeed": "true"}, Spark())


def test_delta_cdf_checks_can_be_disabled_without_skipping_the_read() -> None:
    from structure.plugin.pyspark.delta.runtime import open_delta_relation

    class Row:
        def asDict(self, *, recursive):
            return {"location": "/table", "properties": {}}

    class Detail:
        def first(self):
            return Row()

    class Table:
        def detail(self):
            return Detail()

    class Reader:
        def __init__(self):
            self.options = []

        def format(self, value):
            return self

        def option(self, name, value):
            self.options.append((name, value))
            return self

        def load(self, location):
            self.location = location
            return "cdf-frame"

    class Spark:
        read = Reader()

    result = open_delta_relation(
        Table(),
        "delta_changes",
        1,
        None,
        "IntegerType",
        check_cdf_configuration=False,
        spark=Spark(),
    )
    assert result == "cdf-frame"
    assert Spark.read.location == "/table"
    assert ("readChangeFeed", "true") in Spark.read.options


def test_multiple_delta_effect_steps_keep_source_order() -> None:
    @transform
    class ChangeTwice(Transform):
        orders = delta_table(Order)

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
        orders = delta_table(Order)

        @step(input=(changes, orders), output=orders)
        def merge(self, change: Change, order: Order) -> None:
            (delta_merge(order, change, on=order.id == change.id)
             .when_not_matched_by_source_update(set=Order(status=change.status))
             .execute())

    with pytest.raises(Exception, match="unavailable relation scope"):
        _compile(Invalid)


def test_schema_evolving_merge_uses_to_schema_as_delta_output() -> None:
    changes_input = input(ChangeV2)
    current_orders_input = delta_input(Order)
    orders_output = delta_output(OrderV2)

    @transform
    class Evolve(Transform):
        changes = changes_input
        current_orders = current_orders_input
        orders = orders_output

        def merge(self, change: ChangeV2, order: Order) -> OrderV2:
            return (
                delta_merge(order, change, on=order.id == change.id)
                .with_schema_evolution(to=OrderV2)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

    plan = _compile(Evolve).lowered
    assert tuple(binding.name for binding in plan.inputs) == ("changes", "current_orders")
    step_plan = plan.steps[0]
    assert step_plan.effect
    assert step_plan.delta_mutations[0].schema_evolution
    assert step_plan.delta_mutations[0].output == "orders"
    assert step_plan.delta_mutations[0].output_schema is OrderV2


def test_schema_evolving_delta_table_effect_needs_no_return_statement() -> None:
    @transform
    class Evolve(Transform):
        changes = input(ChangeV2)
        orders = delta_table(Order)

        def merge(self, change: ChangeV2, order: Order) -> None:
            (
                delta_merge(order, change, on=order.id == change.id)
                .with_schema_evolution(to=OrderV2)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

    step_plan = _compile(Evolve).lowered.steps[0]
    mutation = step_plan.delta_mutations[0]
    assert step_plan.effect
    assert mutation.schema_evolution
    assert mutation.output is None
    assert mutation.output_schema is OrderV2


def test_schema_evolution_to_must_match_declared_delta_output() -> None:
    class OtherOrderV2(Order):
        note = string()

    @transform
    class Invalid(Transform):
        changes = input(ChangeV2)
        current_orders = delta_input(Order)
        orders = delta_output(OrderV2)

        def merge(self, change: ChangeV2, order: Order) -> OrderV2:
            return (
                delta_merge(order, change, on=order.id == change.id)
                .with_schema_evolution(to=OtherOrderV2)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

    with pytest.raises(Exception, match="declared delta_output"):
        _compile(Invalid)


def test_schema_evolving_append_is_explicit() -> None:
    changes_input = input(ChangeV2)
    current_orders_input = delta_input(Order)
    orders_output = delta_output(OrderV2)

    @transform
    class Evolve(Transform):
        changes = changes_input
        current_orders = current_orders_input
        orders = orders_output

        def append(self, change: ChangeV2, order: Order) -> OrderV2:
            return delta_append(order, change).with_schema_evolution(to=OrderV2).execute()

    mutation = _compile(Evolve).lowered.steps[0].delta_mutations[0]
    assert mutation.kind == "append"
    assert mutation.schema_evolution
    assert mutation.output == "orders"


def test_evolution_rejects_a_result_field_missing_from_source() -> None:
    @transform
    class Invalid(Transform):
        changes = input(Change)
        current_orders = delta_input(Order)
        orders = delta_output(OrderV2)

        def merge(self, change: Change, order: Order) -> OrderV2:
            return (
                delta_merge(order, change, on=order.id == change.id)
                .with_schema_evolution(to=OrderV2)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

    with pytest.raises(Exception, match="cannot supply that column"):
        _compile(Invalid)
