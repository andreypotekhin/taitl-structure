from typing import cast

import pytest
from testing.model.orders.schemas.query_operations import ClearanceItem, Inventory, Store

from structure.plugin.pyspark import lateral_join
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.operations_api import named_struct
from structure.plugin.pyspark.dsl.RelationQuery import RelationQuery
from structure.plugin.pyspark.dsl.types import BooleanType
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext


def test_relation_query_is_immutable_and_does_not_filter_the_driving_context() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    store = InputScope(name="store", schema=Store)

    original = inventory.where(cast(Expression, inventory.store_id) == cast(Expression, store.store_id).outer())
    filtered = original.filter(cast(Expression, inventory.status) == "Clearance")
    projected = filtered.project(ClearanceItem)

    assert isinstance(original, RelationQuery)
    assert len(original.filters) == 1
    assert len(filtered.filters) == 2
    assert original.projection is None
    assert projected.output_schema is ClearanceItem
    assert projected.quantity.type == Inventory._structure_fields["quantity"].type
    assert filtered.filters[0].args[1].kind == "outer"


def test_query_order_and_limit_are_typed_and_branchable() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    ordered = inventory.where(cast(Expression, inventory.status) == "Clearance").order_by(
        cast(Expression, inventory.inventory_id).desc()
    )
    limited = ordered.limit(1)

    assert ordered.bound is None
    assert limited.bound == 1
    assert ordered.orderings[0].kind == "order"

    with pytest.raises(TypeError, match="preceding query.order_by"):
        inventory.limit(1)
    with pytest.raises(TypeError, match="excluding booleans"):
        ordered.limit(True)


def test_query_exists_and_scalar_membership_capture_the_query() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    query = inventory.where(cast(Expression, inventory.status) == "Clearance").project(ClearanceItem)

    exists = query.exists()
    membership = cast(Expression, inventory.quantity).isin(inventory.project(["quantity"]))

    assert exists.kind == "query_exists"
    assert exists.nullable is False
    assert membership.kind == "isin_query"
    assert cast(RelationQuery, (membership.data or {})["query"]).output_fields[0].name == "quantity"
    assert Expression(kind="field", type=cast(Expression, inventory.quantity).type).outer().kind == "outer"


def test_in_subquery_rejects_correlation_even_without_a_capture_context() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    store = InputScope(name="store", schema=Store)
    query = inventory.where(cast(Expression, inventory.store_id) == cast(Expression, store.store_id).outer())

    with pytest.raises(TypeError, match="uncorrelated queries only"):
        cast(Expression, store.store_id).isin(query)


def test_outer_marker_is_rejected_outside_a_query_recipe() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)

    class AdmittedCapabilities:
        def require(self, requirement) -> None:
            return None

    with pytest.raises(TypeError, match="only inside a typed relation query"):
        MapPySparkExpression().map(
            cast(Expression, inventory.quantity).outer(), capabilities=AdmittedCapabilities()
        )


def test_query_validation_tracks_the_schema_at_each_projection() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test"):
        query = cast(RelationQuery, inventory.project(ClearanceItem)).where(
            cast(Expression, inventory.status) == "Clearance"
        )

        with pytest.raises(TypeError, match="unavailable projected field"):
            query.exists()


def test_query_validation_checks_expression_helper_expansions_for_outer_scopes() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    unrelated = InputScope(name="unrelated", schema=Store)
    expanded = cast(Expression, inventory.status) == "Clearance"
    expanded = expanded & (cast(Expression, unrelated.store_id).outer() == "a")
    helper = Expression(kind="special_expr", type=BooleanType(), data={"expanded": expanded})

    with PySparkSymbolicContext(step="test"):
        query = inventory.where(helper)
        with pytest.raises(TypeError, match="unrelated scope"):
            query.exists()

    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test"):
        query = (
            cast(RelationQuery, inventory.project(ClearanceItem))
            .order_by(cast(Expression, inventory.status))
            .limit(1)
        )

        with pytest.raises(TypeError, match="unavailable projected field"):
            query.exists()


def test_struct_membership_uses_positional_member_types_not_schema_identity() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    query = inventory.project(ClearanceItem)
    value = named_struct("name", "A", "amount", 10)

    membership = value.isin(query)

    assert membership.kind == "isin_query"


def test_struct_membership_requires_matching_projected_column_arity() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    value = named_struct("name", "A", "amount", 10)

    with pytest.raises(TypeError, match="matching projected column arity"):
        value.isin(inventory.project(["item_name"]))


def test_query_projection_rejects_unknown_fields_and_wrong_scalar_arity() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)

    with pytest.raises(TypeError, match="unknown source fields"):
        inventory.project(["missing"])
    with pytest.raises(TypeError, match="exactly one projected"):
        cast(Expression, inventory.quantity).isin(inventory.project(ClearanceItem))


def test_consuming_a_derived_query_also_consumes_its_ancestors() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test") as context:
        query = inventory.where(cast(Expression, inventory.status) == "Clearance")
        query = query.project(ClearanceItem)
        query.exists()

        assert context.unused_relation_queries() == ()


def test_unconsumed_query_is_tracked_for_the_step_diagnostic() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test") as context:
        inventory.where(cast(Expression, inventory.status) == "Clearance")

        unused = context.unused_relation_queries()
        assert len(unused) == 2  # The root input descriptor and its filtered query.


def test_lateral_join_records_query_separately_from_post_query_condition() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    store = InputScope(name="store", schema=Store)
    with PySparkSymbolicContext(step="test") as context:
        context.register_current_scope("store")
        context.register_relation_scope("inventory", inventory)
        joined = lateral_join(
            inventory,
            where=(
                cast(Expression, inventory.store_id) == cast(Expression, store.store_id).outer()
            ) & (cast(Expression, inventory.status) == "Clearance"),
            to=ClearanceItem,
            on=cast(Expression, inventory.quantity) > 50,
            how="left",
        )

        join = context.joins[0]
        assert joined._structure_scope_schema is ClearanceItem
        assert cast(RelationQuery, join.query).output_schema is ClearanceItem
        assert join.predicate.kind == "gt"
        assert join.how.value == "left"
        assert context.operations[0].cardinality_bounds == (1, None)
        assert context.unused_relation_queries() == ()


def test_lateral_join_records_bounds_from_limit_and_join_kind() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test") as context:
        context.register_relation_scope("inventory", inventory)
        lateral_join(
            inventory,
            where=cast(Expression, inventory.status) == "Clearance",
            order_by=(cast(Expression, inventory.quantity).desc(),),
            limit=0,
            allow_cartesian=True,
        )

        assert context.operations[0].cardinality_bounds == (0, 0)

    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test") as context:
        context.register_relation_scope("inventory", inventory)
        lateral_join(
            inventory,
            where=cast(Expression, inventory.status) == "Clearance",
            order_by=(cast(Expression, inventory.quantity).desc(),),
            limit=0,
            how="left",
            allow_cartesian=True,
        )

        assert context.operations[0].cardinality_bounds == (1, 1)


def test_lateral_join_requires_explicit_cartesian_opt_in() -> None:
    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test") as context:
        context.register_relation_scope("inventory", inventory)
        with pytest.raises(TypeError, match="allow_cartesian=True"):
            lateral_join(inventory)

    inventory = InputScope(name="inventory", schema=Inventory)
    with PySparkSymbolicContext(step="test") as context:
        context.register_relation_scope("inventory", inventory)
        lateral_join(inventory, allow_cartesian=True)
        assert context.joins[0].method.value == "lateral_join"


def test_query_expression_renders_native_ordered_subquery_without_classic_access() -> None:
    class AdmittedCapabilities:
        def require(self, requirement) -> None:
            return None

    inventory = InputScope(name="inventory", schema=Inventory)
    store = InputScope(name="store", schema=Store)
    query = (
        inventory.where(cast(Expression, inventory.store_id) == cast(Expression, store.store_id).outer())
        .order_by(cast(Expression, inventory.inventory_id).desc())
        .limit(1)
        .project(ClearanceItem)
    )
    recipe = MapPySparkExpression().map(query.exists(), capabilities=AdmittedCapabilities())

    generated = render_pyspark_expression(recipe, scope_aliases={"store": "stores"})

    assert ".exists()" in generated
    assert ".where(" in generated and ".orderBy(" in generated and ".limit(1)" in generated
    assert generated.index(".where(") < generated.index(".orderBy(") < generated.index(".limit(1)")
    assert ".select(" in generated and ".outer()" in generated
    assert all(access not in generated for access in ("._jdf", "._jvm", ".rdd", ".sparkContext"))


def test_query_expression_traceability_includes_inner_and_outer_reads() -> None:
    class AdmittedCapabilities:
        def require(self, requirement) -> None:
            return None

    inventory = InputScope(name="inventory", schema=Inventory)
    store = InputScope(name="store", schema=Store)
    query = inventory.where(
        (cast(Expression, inventory.store_id) == cast(Expression, store.store_id).outer())
        & (cast(Expression, inventory.status) == "Clearance")
    )
    recipe = MapPySparkExpression().map(query.exists(), capabilities=AdmittedCapabilities())

    assert CompilerDataflowReads().reads(recipe) == (
        "inventory.store_id",
        "store.store_id",
        "inventory.status",
    )
