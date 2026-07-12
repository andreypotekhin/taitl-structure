import sys
from dataclasses import dataclass, replace
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

import structure
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.runtime.execution.online.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform
from structure.app.runtime.execution.online.logic.PySparkExpressionEvaluator import PySparkExpressionEvaluator
from structure.app.runtime.execution.online.logic.PySparkFrameValidator import PySparkFrameValidator
from structure.app.runtime.session.model.TransformResult import TransformResult
from structure.app.target.capabilities.model.BackendId import BackendId
from structure.app.target.pyspark.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.app.target.pyspark.model.PySparkAggregateKey import PySparkAggregateKey
from structure.app.target.pyspark.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.app.target.pyspark.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.model.PySparkInputRecipe import PySparkInputRecipe
from structure.app.target.pyspark.model.PySparkJoinAsOfRecipe import PySparkJoinAsOfRecipe
from structure.app.target.pyspark.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkJoinTemporalRecipe import PySparkJoinTemporalRecipe
from structure.app.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.app.target.pyspark.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.target.pyspark.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.app.target.pyspark.model.PySparkWatermarkRecipe import PySparkWatermarkRecipe


class RawOrder(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    status = structure.field(structure.String(), nullable=True)


class PublishedOrder(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    status = structure.field(structure.String(), nullable=True)


class Address(structure.Structure):
    city = structure.field(structure.String(), nullable=False)
    postal_code = structure.field(structure.String(), nullable=False)


class RawShippedOrder(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    shipping = structure.field(structure.Struct(Address), nullable=True)


class PublishedShippedOrder(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    shipping = structure.field(structure.Struct(Address), nullable=False)


class Customer(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    segment = structure.field(structure.String(), nullable=True)
    valid_from = structure.field(structure.String(), nullable=False)
    valid_to = structure.field(structure.String(), nullable=True)


class PublishedOrderId(structure.Structure):
    id = structure.field(structure.String(), nullable=False)


class PublishedOrderStatus(structure.Structure):
    status = structure.field(structure.String(), nullable=True)


class RawMetric(structure.Structure):
    customer_id = structure.field(structure.String(), nullable=False)
    quantity = structure.field(structure.Long(), nullable=False)


class RawTagBatch(structure.Structure):
    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


class RawMapBatch(structure.Structure):
    attributes = structure.field(
        structure.Map(structure.String(), structure.String(), value_contains_null=True), nullable=True
    )


class CustomerMetric(structure.Structure):
    customer_id = structure.field(structure.String(), nullable=False)
    order_count = structure.field(structure.Long(), nullable=False)
    distinct_customers = structure.field(structure.Long(), nullable=False)
    quantity = structure.field(structure.Long(), nullable=False)
    min_quantity = structure.field(structure.Long(), nullable=False)
    max_quantity = structure.field(structure.Long(), nullable=False)
    avg_quantity = structure.field(structure.Double(), nullable=False)


class PermissivePublishedOrder(PublishedOrder):
    pass


def test_online_expression_evaluator_preserves_pyspark_column_semantics() -> None:
    """I can rely on online execution and generated execution to consume the same PySpark semantic contract."""

    evaluator = PySparkExpressionEvaluator()
    functions = FakeFunctions("functions")
    aliases = {RawOrder.__name__: "orders"}

    cases = [
        (_is_null(_field(RawOrder, "status")), "col(orders.status).isNull()"),
        (
            _binary("and", _not_null(_field(RawOrder, "id")), _is_null(_field(RawOrder, "status"))),
            "(col(orders.id).isNotNull() AND col(orders.status).isNull())",
        ),
        (
            _binary("or", _is_null(_field(RawOrder, "status")), _not_null(_field(RawOrder, "id"))),
            "(col(orders.status).isNull() OR col(orders.id).isNotNull())",
        ),
        (_binary("eq", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) == lit('A-1'))"),
        (_binary("ne", _field(RawOrder, "status"), _literal("cancelled")), "(col(orders.status) != lit('cancelled'))"),
        (_binary("gt", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) > lit('A-1'))"),
        (_binary("lt", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) < lit('A-1'))"),
        (_binary("le", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) <= lit('A-1'))"),
        (_binary("ge", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) >= lit('A-1'))"),
        (_binary("add", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) + lit('A-1'))"),
        (_binary("sub", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) - lit('A-1'))"),
        (_binary("mul", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) * lit('A-1'))"),
        (
            _binary("null_safe_eq", _field(RawOrder, "status"), _literal(None)),
            "col(orders.status).eqNullSafe(lit(None))",
        ),
        (
            _isin(_field(RawOrder, "status"), _literal("new"), _literal("held")),
            "col(orders.status).isin(lit('new'),lit('held'))",
        ),
        (_string_predicate("contains", _field(RawOrder, "status"), "new"), "col(orders.status).contains('new')"),
        (_string_predicate("like", _field(RawOrder, "status"), "new%"), "col(orders.status).like('new%')"),
        (
            _string_predicate("ilike", _field(RawOrder, "status"), "NEW%"),
            "col(orders.status).ilike('NEW%')",
        ),
        (
            _string_predicate("rlike", _field(RawOrder, "status"), r"release-[0-9]+"),
            "col(orders.status).rlike('release-[0-9]+')",
        ),
        (_item(_field(RawTagBatch, "tags"), _literal(0)), "col(RawTagBatch.tags)[0]"),
        (_item(_field(RawMapBatch, "attributes"), _literal("region")), "col(RawMapBatch.attributes)['region']"),
        (_get_field(_field(RawShippedOrder, "shipping"), "city"), "col(RawShippedOrder.shipping).getField('city')"),
        (_cast(_field(RawOrder, "status"), "int"), "cast(col(orders.status) as int)"),
        (_try_cast(_field(RawOrder, "status"), "int"), "try_cast(col(orders.status) as int)"),
        (_call("substring", _field(RawOrder, "status"), start=1, length=3), "substring(col(orders.status),1,3)"),
        (_call("split", _field(RawOrder, "status"), pattern="-", limit=-1), "split(col(orders.status),'-',-1)"),
        (
            _call("regexp_replace", _field(RawOrder, "status"), pattern=r"\s+", replacement=" "),
            "regexp_replace(col(orders.status),'\\\\s+',' ')",
        ),
        (
            _call("regexp_extract", _field(RawOrder, "status"), pattern=r"^([^-]+)", group=1),
            "regexp_extract(col(orders.status),'^([^-]+)',1)",
        ),
        (_call("length", _field(RawOrder, "status")), "length(col(orders.status))"),
        (_call("initcap", _field(RawOrder, "status")), "initcap(col(orders.status))"),
        (_call("reverse", _field(RawOrder, "status")), "reverse(col(orders.status))"),
        (
            _call("translate", _field(RawOrder, "status"), matching="-", replacement="_"),
            "translate(col(orders.status),'-','_')",
        ),
        (_call("instr", _field(RawOrder, "status"), substring="-"), "instr(col(orders.status),'-')"),
        (
            _call("levenshtein", _field(RawOrder, "status"), _literal("release")),
            "levenshtein(col(orders.status),lit('release'))",
        ),
        (
            _call("concat_ws", _field(RawOrder, "status"), _literal("release"), separator=" / "),
            "concat_ws(' / ',col(orders.status),lit('release'))",
        ),
        (_call("date_add", _field(RawOrder, "status"), days=7), "date_add(col(orders.status),7)"),
        (
            _call("datediff", _field(RawOrder, "id"), _field(RawOrder, "status")),
            "datediff(col(orders.id),col(orders.status))",
        ),
        (_call("date_trunc", _field(RawOrder, "status"), unit="month"), "date_trunc('month',col(orders.status))"),
        (_call("abs", _field(RawOrder, "status")), "abs(col(orders.status))"),
        (_call("round", _field(RawOrder, "status"), scale=1), "round(col(orders.status),1)"),
        (_call("ceil", _field(RawOrder, "status")), "ceil(col(orders.status))"),
        (_call("floor", _field(RawOrder, "status")), "floor(col(orders.status))"),
        (_not(_is_null(_field(RawOrder, "status"))), "~(col(orders.status).isNull())"),
        (_is_nan(_field(RawOrder, "status")), "isnan(col(orders.status))"),
        (_call("upper", _call("trim", _field(RawOrder, "status"))), "upper(trim(col(orders.status)))"),
        (
            _array_filter(
                _array_transform(_field(RawTagBatch, "tags"), _call("lower", _call("trim", _lambda_item()))),
                _not_null(_lambda_item()),
            ),
            "filter(transform(col(RawTagBatch.tags), lambda item: lower(trim(item))), lambda item: item.isNotNull())",
        ),
        (
            _map_filter(
                _map_transform_values(
                    _field(RawMapBatch, "attributes"), _call("lower", _call("trim", _lambda_value()))
                ),
                _not_null(_lambda_value()),
            ),
            "map_filter(transform_values(col(RawMapBatch.attributes), "
            "lambda key, value: lower(trim(value))), lambda key, value: value.isNotNull())",
        ),
        (
            _when(_binary("ge", _field(RawOrder, "id"), _literal("M")), _literal("large"), _literal("standard")),
            "when((col(orders.id) >= lit('M')), lit('large')).otherwise(lit('standard'))",
        ),
        (_to_decimal(_field(RawOrder, "status"), precision=12, scale=2), "cast(col(orders.status) as decimal(12,2))"),
    ]

    assert [evaluator.evaluate(recipe, functions=functions, aliases=aliases).expression for recipe, _ in cases] == [
        expected for _, expected in cases
    ]


def test_online_expression_evaluator_builds_nested_struct_columns() -> None:
    evaluator = PySparkExpressionEvaluator()
    functions = FakeFunctions("functions")
    expression = PySparkExpressionRecipe(
        kind="struct",
        type=structure.Struct(Address),
        nullable=False,
        data={"fields": tuple(Address._structure_fields.values())},
        args=(
            _field_path(RawShippedOrder, "shipping", "city"),
            _field_path(RawShippedOrder, "shipping", "postal_code"),
        ),
    )

    column = evaluator.evaluate(expression, functions=functions, aliases={RawShippedOrder.__name__: "orders"})

    assert column.expression == "struct(city=col(orders.shipping.city),postal_code=col(orders.shipping.postal_code))"


def test_online_expression_evaluator_preserves_window_projection_semantics() -> None:
    evaluator = PySparkExpressionEvaluator()
    functions = FakeFunctions("functions")
    aliases = {RawMetric.__name__: "metrics"}
    quantity = _field(RawMetric, "quantity")
    customer_id = _field(RawMetric, "customer_id")

    cases = [
        (
            _window("row_number", partition_by=customer_id, order_by=quantity),
            "row_number().over(partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).asc()))",
        ),
        (
            _window("rank", partition_by=customer_id, order_by=quantity, descending=True),
            "rank().over(partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).desc()))",
        ),
        (
            _window("dense_rank", partition_by=customer_id, order_by=_order(quantity, "asc_nulls_last")),
            "dense_rank().over(partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).asc_nulls_last()))",
        ),
        (
            _window("dense_rank", partition_by=customer_id, order_by=quantity),
            "dense_rank().over(partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).asc()))",
        ),
        (
            _window("lag", value=quantity, partition_by=customer_id, order_by=quantity),
            "lag(col(metrics.quantity),1).over(partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).asc()))",
        ),
        (
            _window("lead", value=quantity, partition_by=customer_id, order_by=quantity),
            "lead(col(metrics.quantity),1).over(partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).asc()))",
        ),
        (
            _window("rolling_sum", value=quantity, partition_by=customer_id, order_by=quantity, preceding=2),
            "sum(col(metrics.quantity)).over("
            "partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).asc()).rowsBetween(-2,0))",
        ),
        (
            _window("rolling_avg", value=quantity, partition_by=customer_id, order_by=quantity, preceding=2),
            "avg(col(metrics.quantity)).over("
            "partitionBy(col(metrics.customer_id)).orderBy(col(metrics.quantity).asc()).rowsBetween(-2,0))",
        ),
    ]

    assert [
        evaluator.evaluate(recipe, functions=functions, aliases=aliases, window=FakeWindow).expression
        for recipe, _ in cases
    ] == [expected for _, expected in cases]


def test_online_runner_executes_lowered_pyspark_recipe(monkeypatch) -> None:
    """I can rely on online execution and generated execution to consume the same PySpark semantic contract."""

    functions = FakeFunctions("pyspark.sql.functions")
    _install_fake_pyspark(monkeypatch, functions)

    source = FakeFrame(
        "source",
        FakeSchema(
            (
                FakeField("id", FakeTypes.StringType(), False),
                FakeField("status", FakeTypes.StringType(), True),
            )
        ),
    )
    result = RunOnlinePySparkTransform()(
        cast(Any, FakeInvocation(orders=source)),
        _online_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark=object(),
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.field_names == ["id", "status"]
    assert published.operations == (
        "alias:orders",
        "where:col(orders.status).isNotNull()",
        "select:id=col(orders.id),status=lower(trim(col(orders.status)))",
        "alias:published",
    )


def test_online_runner_applies_source_watermark_in_recipe_order(monkeypatch) -> None:
    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    plan = _with_operations(
        _online_plan(),
        PySparkOperationRecipe.watermark_operation(
            PySparkWatermarkRecipe(_field_scope("orders", RawOrder, "id"), "10 minutes")
        ),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, FakeInvocation(orders=_frame("orders", RawOrder))),
        plan,
        session=SimpleNamespace(
            online_executor=None,
            spark=object(),
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    assert cast(FakeFrame, result.published).operations[:2] == (
        "alias:orders",
        "withWatermark:id:10 minutes",
    )


def test_online_runner_applies_joined_input_watermark_before_join(monkeypatch) -> None:
    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    plan = _with_operations(
        _join_and_hook_plan(),
        PySparkOperationRecipe.watermark_operation(
            PySparkWatermarkRecipe(_field_scope("customers", Customer, "id"), "20 minutes")
        ),
        PySparkOperationRecipe.join_operation(_join_and_hook_plan().steps[0].joins[0]),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, FakeInvocation(orders=_frame("orders", RawOrder), customers=_frame("customers", Customer))),
        plan,
        session=SimpleNamespace(
            online_executor=None,
            spark=object(),
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    operations = cast(FakeFrame, result.published).operations
    assert operations.index("withWatermark:id:20 minutes") < next(
        index for index, operation in enumerate(operations) if operation.startswith("join:customers:left:")
    )


def test_online_runner_preserves_injected_multi_output_result_contract() -> None:
    """I can call run(session) on a transform invocation so StructureSession chooses the runtime runner."""

    result = TransformResult({"ids": "ids-frame", "statuses": "statuses-frame"})
    invocation = FakeInvocation(orders="orders-frame")
    session = SimpleNamespace(
        online_executor=lambda **_: result,
        spark=None,
        ctx=None,
        execution_mode="online",
        target_backend="pyspark",
    )

    assert RunOnlinePySparkTransform()(cast(Any, invocation), _multi_result_plan(), session=session) is result

    session.online_executor = lambda **_: "one-frame"
    with pytest.raises(TypeError, match="TransformResult for multi-output"):
        RunOnlinePySparkTransform()(cast(Any, invocation), _multi_result_plan(), session=session)


def test_online_runner_applies_step_hooks_and_step_and_output_joins(monkeypatch) -> None:
    """I can rely on online execution and generated execution to preserve the same transform semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _join_and_hook_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx={"run": "online"},
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert invocation.hook_calls == [
        ("prepare_orders", "orders", "spark", {"run": "online"}),
        ("record_published", "orders", "spark", {"run": "online"}),
    ]
    assert published.operations == (
        "before-hook",
        "alias:orders",
        "join:customers:left:col(orders.id).eqNullSafe(col(customers.id))",
        "select:id=col(orders.id),status=coalesce(col(customers.segment),col(orders.status))",
        "after-hook",
        "select:id=col(id),status=col(status)",
        "alias:published",
        "join:customers:inner:(col(published.id) == col(customers.id))",
        "where:col(published.id).isNotNull()",
        "select:id=col(published.id),status=col(published.status)",
    )


def test_online_runner_applies_existence_join_modes(monkeypatch) -> None:
    """I can rely on online and generated execution to share v2 existence join semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _existence_join_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "join:customers:left_semi:(col(orders.id) == col(customers.id))",
        "select:id=col(orders.id),status=col(orders.status)",
        "alias:published",
    )


def test_online_runner_applies_inner_join_as_row_multiplying_join(monkeypatch) -> None:
    """I can rely on online and generated execution to share v2 row-multiplying join semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _inner_join_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "join:customers:inner:(col(orders.id) == col(customers.id))",
        "select:id=col(orders.id),status=col(customers.segment)",
        "alias:published",
    )


def test_online_runner_applies_grouped_aggregate_recipe(monkeypatch) -> None:
    """I can rely on online and generated execution to share v2 aggregate semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(metrics=_frame("metrics", RawMetric))

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _aggregate_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    totals = cast(FakeFrame, result.totals)

    assert totals.operations == (
        "alias:metrics",
        "groupBy:customer_id=col(metrics.customer_id)",
        "agg:order_count=cast(count(lit(1)) as LongType()),"
        "distinct_customers=cast(countDistinct(col(metrics.customer_id)) as LongType()),"
        "quantity=cast(sum(col(metrics.quantity)) as LongType()),"
        "min_quantity=cast(min(col(metrics.quantity)) as LongType()),"
        "max_quantity=cast(max(col(metrics.quantity)) as LongType()),"
        "avg_quantity=cast(avg(col(metrics.quantity)) as DoubleType())",
        "select:customer_id=col(customer_id),order_count=col(order_count),"
        "distinct_customers=col(distinct_customers),quantity=col(quantity),"
        "min_quantity=col(min_quantity),max_quantity=col(max_quantity),avg_quantity=col(avg_quantity)",
        "alias:totals",
    )


def test_online_runner_applies_selected_row_window_recipe(monkeypatch) -> None:
    """I can rely on online and generated execution to share v2 selected-row semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(orders=_frame("orders", RawOrder))

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _selected_row_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "withColumn:__structure_publish_latest_rank="
        "row_number().over(partitionBy(col(orders.id)).orderBy(col(orders.status).desc()))",
        "where:(col(__structure_publish_latest_rank) == lit(1))",
        "drop:__structure_publish_latest_rank",
        "select:id=col(orders.id),status=col(orders.status)",
        "alias:published",
    )


def test_online_runner_applies_exact_duplicate_removal_recipe(monkeypatch) -> None:
    """I can rely on online and generated execution to share exact duplicate cleanup semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(orders=_frame("orders", RawOrder))

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _drop_duplicates_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "dropDuplicates",
        "select:id=col(orders.id),status=col(orders.status)",
        "alias:published",
    )


def test_online_runner_applies_subset_duplicate_removal_recipe(monkeypatch) -> None:
    """I can use PySpark-compatible subset duplicate cleanup when representative rows are acceptable."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(orders=_frame("orders", RawOrder))

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _drop_duplicates_plan(subset=True),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "dropDuplicates:id",
        "select:id=col(orders.id),status=col(orders.status)",
        "alias:published",
    )


def test_online_runner_applies_relation_duplicate_removal_before_join(monkeypatch) -> None:
    """I can dedupe a relation source before a later join consumes it."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _relation_drop_duplicates_join_plan(before_join=True),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "dropDuplicates:id",
        "join:customers:left:(col(orders.id) == col(customers.id))",
        "select:id=col(orders.id),status=col(customers.segment)",
        "alias:published",
    )


def test_online_runner_applies_relation_duplicate_removal_after_join(monkeypatch) -> None:
    """I can place relation-scoped dedupe after a join and have source order preserved."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _relation_drop_duplicates_join_plan(before_join=False),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "join:customers:left:(col(orders.id) == col(customers.id))",
        "dropDuplicates:id",
        "select:id=col(orders.id),status=col(customers.segment)",
        "alias:published",
    )


def test_online_runner_dedupes_lookup_input_deterministically(monkeypatch) -> None:
    """I can rely on online and generated execution to share v2 deduped lookup semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _deduped_join_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "withColumn:__structure_customers_rank=row_number().over(partitionBy(col(customers.id)).orderBy(col(customers.segment).desc()))",
        "where:(col(__structure_customers_rank) == lit(1))",
        "drop:__structure_customers_rank",
        "join:customers:left:(col(orders.id) == col(customers.id))",
        "select:id=col(orders.id),status=col(customers.segment)",
        "alias:published",
    )
    assert invocation._structure_bound_inputs["customers"].operations == ()


def test_online_runner_applies_temporal_closed_open_lookup(monkeypatch) -> None:
    """I can rely on online and generated execution to share v2 temporal lookup semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _temporal_join_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "join:customers:left:(((col(orders.id) == col(customers.id)) AND (col(customers.valid_from) <= col(orders.status))) AND ((col(orders.status) < col(customers.valid_to)) OR col(customers.valid_to).isNull()))",
        "select:id=col(orders.id),status=col(customers.segment)",
        "alias:published",
    )


def test_online_runner_applies_backward_as_of_lookup(monkeypatch) -> None:
    """I can rely on online and generated execution to share v2 as-of lookup semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _as_of_join_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.operations == (
        "alias:orders",
        "withColumn:__structure_orders_customers_row=monotonically_increasing_id()",
        "join:customers:left:((col(orders.id) == col(customers.id)) AND (col(customers.valid_from) <= col(orders.status)))",
        "withColumn:__structure_customers_as_of_rank=row_number().over(partitionBy(col(__structure_orders_customers_row)).orderBy(col(customers.valid_from).desc()))",
        "where:(col(__structure_customers_as_of_rank) == lit(1))",
        "drop:__structure_customers_as_of_rank",
        "drop:__structure_orders_customers_row",
        "select:id=col(orders.id),status=col(customers.segment)",
        "alias:published",
    )


def test_online_runner_materializes_multiple_step_results(monkeypatch) -> None:
    """I can run online/generated parity tests for every supported compiled operation."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(orders=_frame("orders", RawOrder))

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _multi_result_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    ids = cast(FakeFrame, result.ids)
    statuses = cast(FakeFrame, result.statuses)

    assert invocation.hook_calls == [("record_ids", "orders", "spark", None)]
    assert ids.field_names == ["id"]
    assert ids.operations == (
        "alias:orders",
        "select:id=col(orders.id)",
        "ids-hook",
        "select:id=col(id)",
        "alias:ids",
    )
    assert statuses.field_names == ["status"]
    assert statuses.operations == ("alias:orders", "select:status=col(orders.status)", "alias:statuses")


def test_online_schema_validation_projects_equivalent_spark_shapes() -> None:
    """Online execution exposes equivalent Spark schemas."""

    validator = PySparkFrameValidator()
    frame = FakeFrame(
        "published",
        FakeSchema(
            (
                FakeField("id", FakeTypes.StringType(), False),
                FakeField("status", FakeTypes.StringType(), True),
                FakeField("debug", FakeTypes.StringType(), True),
            )
        ),
    )
    validation = PySparkValidationRecipe(
        target="published",
        schema=PermissivePublishedOrder,
        mode=structure.SchemaMode.ALLOW_EXTRA_COLUMNS,
        project=True,
        reason="hook",
    )

    validator.validate(frame, validation, types=FakeTypes)
    projected = validator.project(frame, validation, types=FakeTypes, functions=FakeFunctions("functions"))

    assert projected.field_names == ["id", "status"]
    assert projected.operations == ("select:id=col(id),status=col(status)",)


def test_online_schema_validation_accepts_spark_collection_nullability_metadata() -> None:
    """Spark can report conservative collection nullability for equivalent physical schemas."""

    validation = PySparkValidationRecipe(
        target="tags",
        schema=RawTagBatch,
        mode=structure.SchemaMode.STRICT,
        project=False,
        reason="output",
    )
    frame = FakeFrame(
        "tags",
        FakeSchema(
            (
                FakeField(
                    "tags",
                    FakeTypes.ArrayType(FakeTypes.StringType(), containsNull=True),
                    True,
                ),
            )
        ),
    )

    PySparkFrameValidator().validate(frame, validation, types=FakeTypes)


def test_online_schema_validation_rejects_nested_struct_shape_drift() -> None:
    validation = PySparkValidationRecipe(
        target="published",
        schema=PublishedShippedOrder,
        mode=structure.SchemaMode.STRICT,
        project=False,
        reason="output",
    )
    frame = FakeFrame(
        "published",
        FakeSchema(
            (
                FakeField("id", FakeTypes.StringType(), False),
                FakeField(
                    "shipping",
                    cast(FakeType, FakeSchema((FakeField("city", FakeTypes.StringType(), False),))),
                    False,
                ),
            )
        ),
    )

    with pytest.raises(ValueError, match="PublishedShippedOrder.shipping expected"):
        PySparkFrameValidator().validate(frame, validation, types=FakeTypes)


def test_online_schema_validation_rejects_strict_shape_drift() -> None:
    """I can rely on online execution and generated execution to preserve the same transform semantics."""

    validation = PySparkValidationRecipe(
        target="published",
        schema=PublishedOrder,
        mode=structure.SchemaMode.STRICT,
        project=False,
        reason="output",
    )
    frame = FakeFrame(
        "published",
        FakeSchema(
            (
                FakeField("id", FakeTypes.StringType(), False),
                FakeField("status", FakeTypes.StringType(), True),
                FakeField("debug", FakeTypes.StringType(), True),
            )
        ),
    )

    with pytest.raises(ValueError, match="unexpected column\\(s\\): debug"):
        PySparkFrameValidator().validate(frame, validation, types=FakeTypes)


def _online_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _call("lower", _call("trim", _field(RawOrder, "status"))),
        ),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(_not_null(_field(RawOrder, "status")),),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(published_validation,),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(PySparkInputRecipe("orders", RawOrder, 0, input_validation),),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _with_operations(plan: PySparkExecutionPlan, *operations: PySparkOperationRecipe) -> PySparkExecutionPlan:
    step = replace(plan.steps[0], operations=operations)
    return replace(plan, steps=(step,))


def _join_and_hook_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projected_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, True, "hook_projected"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _call("coalesce", _field_scope("customers", Customer, "segment"), _field(RawOrder, "status")),
        ),
    )
    output_projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(PublishedOrder, "id")),
        PySparkProjectionRecipe(PublishedOrder._structure_fields["status"], _field(PublishedOrder, "status")),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(_hook("prepare_orders", lanes=("orders",), outputs=("orders",)),),
        filters=(),
        joins=(
            PySparkJoinRecipe(
                input_name="customers",
                source="customers",
                input_schema=Customer,
                left_alias="orders",
                right_alias="customers",
                how=structure.Join.LEFT,
                hint=structure.JoinHint.BROADCAST,
                predicate=_binary("null_safe_eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
                occurrence=0,
            ),
        ),
        projection=projection,
        after_hooks=(_hook("record_published", lanes=("published",), outputs=("published",)),),
        validations=(projected_validation,),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                joins=(
                    PySparkJoinRecipe(
                        input_name="customers",
                        source="customers",
                        input_schema=Customer,
                        left_alias="published",
                        right_alias="customers",
                        how=structure.Join.INNER,
                        hint=structure.JoinHint.BROADCAST,
                        predicate=_binary(
                            "eq", _field(PublishedOrder, "id"), _field_scope("customers", Customer, "id")
                        ),
                        occurrence=0,
                    ),
                ),
                filters=(_not_null(_field(PublishedOrder, "id")),),
                projection=output_projection,
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _existence_join_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(PublishedOrder._structure_fields["status"], _field(RawOrder, "status")),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(
            PySparkOperationRecipe.join_operation(
                PySparkJoinRecipe(
                    input_name="customers",
                    source="customers",
                    input_schema=Customer,
                    left_alias="orders",
                    right_alias="customers",
                    how=structure.Join.INNER,
                    hint=None,
                    predicate=_binary("eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
                    occurrence=0,
                    method=JoinMethod.EXISTS,
                )
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishKnownCustomers",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _inner_join_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _field_scope("customers", Customer, "segment"),
        ),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(
            PySparkOperationRecipe.join_operation(
                PySparkJoinRecipe(
                    input_name="customers",
                    source="customers",
                    input_schema=Customer,
                    left_alias="orders",
                    right_alias="customers",
                    how=structure.Join.INNER,
                    hint=None,
                    strategy=structure.JoinStrategy.SHUFFLE_HASH,
                    predicate=_binary("eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
                    occurrence=0,
                    method=JoinMethod.ROWSET,
                )
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishKnownCustomers",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _aggregate_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("metrics", RawMetric, structure.SchemaMode.STRICT, False, "input")
    total_validation = PySparkValidationRecipe("totals", CustomerMetric, structure.SchemaMode.STRICT, False, "output")
    aggregate = PySparkAggregateRecipe(
        keys=(
            PySparkAggregateKey(
                name="customer_id",
                expression=_field(RawMetric, "customer_id"),
            ),
        ),
        assignments=(
            PySparkAggregateAssignment(
                field=CustomerMetric._structure_fields["customer_id"],
                function="key",
                expression=_field(RawMetric, "customer_id"),
                key="customer_id",
            ),
            PySparkAggregateAssignment(
                field=CustomerMetric._structure_fields["order_count"],
                function="count",
            ),
            PySparkAggregateAssignment(
                field=CustomerMetric._structure_fields["distinct_customers"],
                function="count_distinct",
                expression=_field(RawMetric, "customer_id"),
            ),
            PySparkAggregateAssignment(
                field=CustomerMetric._structure_fields["quantity"],
                function="sum",
                expression=_field(RawMetric, "quantity"),
            ),
            PySparkAggregateAssignment(
                field=CustomerMetric._structure_fields["min_quantity"],
                function="min",
                expression=_field(RawMetric, "quantity"),
            ),
            PySparkAggregateAssignment(
                field=CustomerMetric._structure_fields["max_quantity"],
                function="max",
                expression=_field(RawMetric, "quantity"),
            ),
            PySparkAggregateAssignment(
                field=CustomerMetric._structure_fields["avg_quantity"],
                function="avg",
                expression=_field(RawMetric, "quantity"),
            ),
        ),
    )
    step = PySparkStepRecipe(
        name="summarize",
        ordinal=0,
        source="metrics",
        source_scope="metrics",
        input_schema=RawMetric,
        output_schema=CustomerMetric,
        input_alias="metrics",
        output_alias="totals",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=(),
        after_hooks=(),
        validations=(total_validation,),
        aggregate=aggregate,
        results=(
            PySparkStepResultRecipe(
                schema=CustomerMetric,
                lane="totals",
                frame="totals",
                output_alias="totals",
                projection=(),
                ordinal=0,
                after_hooks=(),
                validations=(total_validation,),
                aggregate=aggregate,
            ),
        ),
        operations=(PySparkOperationRecipe.aggregate_operation(aggregate),),
    )
    return PySparkExecutionPlan(
        transform="CustomerMetricTotals",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(PySparkInputRecipe("metrics", RawMetric, 0, input_validation),),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="totals",
                ordinal=0,
                source="totals",
                source_scope="totals",
                input_schema=CustomerMetric,
                output_schema=CustomerMetric,
                input_alias="totals",
                output_alias="totals",
                filters=(),
                joins=(),
                projection=(),
                validation=total_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _selected_row_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(PublishedOrder._structure_fields["status"], _field(RawOrder, "status")),
    )
    selected_rows = PySparkSelectedRowsRecipe(
        direction="latest",
        order_by=_field(RawOrder, "status"),
        partition_by=(_field(RawOrder, "id"),),
        ties=structure.TiePolicy.ERROR,
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(published_validation,),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(PySparkOperationRecipe.selected_rows_operation(selected_rows),),
    )
    return PySparkExecutionPlan(
        transform="LatestOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(PySparkInputRecipe("orders", RawOrder, 0, input_validation),),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _drop_duplicates_plan(*, subset: bool = False) -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(PublishedOrder._structure_fields["status"], _field(RawOrder, "status")),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(published_validation,),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(
            PySparkOperationRecipe.drop_duplicates_operation(
                PySparkDuplicateRowsRecipe(subset=(_field(RawOrder, "id"),) if subset else ())
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="UniqueOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(PySparkInputRecipe("orders", RawOrder, 0, input_validation),),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _deduped_join_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _field_scope("customers", Customer, "segment"),
        ),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(
            PySparkOperationRecipe.join_operation(
                PySparkJoinRecipe(
                    input_name="customers",
                    source="customers",
                    input_schema=Customer,
                    left_alias="orders",
                    right_alias="customers",
                    how=structure.Join.LEFT,
                    hint=None,
                    predicate=_binary("eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
                    occurrence=0,
                    dedupe=PySparkJoinDedupeRecipe(
                        order_by=_field_scope("customers", Customer, "segment"),
                        direction="latest",
                        ties=structure.TiePolicy.ERROR,
                    ),
                )
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishKnownCustomers",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _relation_drop_duplicates_join_plan(*, before_join: bool) -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _field_scope("customers", Customer, "segment"),
        ),
    )
    join = PySparkJoinRecipe(
        input_name="customers",
        source="customers",
        input_schema=Customer,
        left_alias="orders",
        right_alias="customers",
        how=structure.Join.LEFT,
        hint=None,
        predicate=_binary("eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
        occurrence=0,
    )
    join_operation = PySparkOperationRecipe.join_operation(join)
    dedupe = PySparkOperationRecipe.drop_duplicates_operation(
        PySparkDuplicateRowsRecipe(subset=(_field_scope("customers", Customer, "id"),), scope="customers")
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(join,),
        projection=projection,
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(dedupe, join_operation) if before_join else (join_operation, dedupe),
    )
    return PySparkExecutionPlan(
        transform="PublishKnownCustomers",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _temporal_join_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _field_scope("customers", Customer, "segment"),
        ),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(
            PySparkOperationRecipe.join_operation(
                PySparkJoinRecipe(
                    input_name="customers",
                    source="customers",
                    input_schema=Customer,
                    left_alias="orders",
                    right_alias="customers",
                    how=structure.Join.LEFT,
                    hint=None,
                    predicate=_binary("eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
                    occurrence=0,
                    method=JoinMethod.TEMPORAL_ONE,
                    temporal=PySparkJoinTemporalRecipe(
                        at=_field(RawOrder, "status"),
                        valid_from=_field_scope("customers", Customer, "valid_from"),
                        valid_to=_field_scope("customers", Customer, "valid_to"),
                    ),
                )
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishKnownCustomers",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _as_of_join_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, structure.SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe(
        "published", PublishedOrder, structure.SchemaMode.STRICT, False, "output"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _field_scope("customers", Customer, "segment"),
        ),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
        operations=(
            PySparkOperationRecipe.join_operation(
                PySparkJoinRecipe(
                    input_name="customers",
                    source="customers",
                    input_schema=Customer,
                    left_alias="orders",
                    right_alias="customers",
                    how=structure.Join.LEFT,
                    hint=None,
                    predicate=_binary("eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
                    occurrence=0,
                    method=JoinMethod.AS_OF_ONE,
                    as_of=PySparkJoinAsOfRecipe(
                        left_time=_field(RawOrder, "status"),
                        right_time=_field_scope("customers", Customer, "valid_from"),
                        direction=structure.AsOf.BACKWARD,
                    ),
                )
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishKnownCustomers",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _multi_result_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, structure.SchemaMode.STRICT, False, "input")
    id_validation = PySparkValidationRecipe("ids", PublishedOrderId, structure.SchemaMode.STRICT, True, "output")
    status_validation = PySparkValidationRecipe(
        "statuses", PublishedOrderStatus, structure.SchemaMode.STRICT, False, "output"
    )
    id_projection = (PySparkProjectionRecipe(PublishedOrderId._structure_fields["id"], _field(RawOrder, "id")),)
    status_projection = (
        PySparkProjectionRecipe(PublishedOrderStatus._structure_fields["status"], _field(RawOrder, "status")),
    )
    step = PySparkStepRecipe(
        name="split",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=(),
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrderId,
                lane="ids",
                frame="ids",
                output_alias="ids",
                projection=id_projection,
                ordinal=0,
                after_hooks=(_hook("record_ids", lanes=("ids",), outputs=("ids",)),),
                validations=(id_validation,),
            ),
            PySparkStepResultRecipe(
                schema=PublishedOrderStatus,
                lane="statuses",
                frame="statuses",
                output_alias="statuses",
                projection=status_projection,
                ordinal=1,
                after_hooks=(),
                validations=(status_validation,),
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="SplitOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(PySparkInputRecipe("orders", RawOrder, 0, input_validation),),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="ids",
                ordinal=0,
                source="ids",
                source_scope="ids",
                input_schema=PublishedOrderId,
                output_schema=PublishedOrderId,
                input_alias="ids",
                output_alias="ids",
                filters=(),
                joins=(),
                projection=(),
                validation=id_validation,
            ),
            PySparkOutputRecipe(
                name="statuses",
                ordinal=1,
                source="statuses",
                source_scope="statuses",
                input_schema=PublishedOrderStatus,
                output_schema=PublishedOrderStatus,
                input_alias="statuses",
                output_alias="statuses",
                filters=(),
                joins=(),
                projection=(),
                validation=status_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _field(schema: type[structure.Structure], name: str) -> PySparkExpressionRecipe:
    return _field_scope(schema.__name__, schema, name)


def _field_scope(scope: str, schema: type[structure.Structure], name: str) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        kind="field",
        type=schema._structure_fields[name].type,
        nullable=schema._structure_fields[name].nullable,
        data={"scope": scope, "field": name},
    )


def _field_path(schema: type[structure.Structure], *path: str) -> PySparkExpressionRecipe:
    field = schema._structure_fields[path[0]]
    type_ = field.type
    nullable = field.nullable
    for name in path[1:]:
        nested = type_.schema._structure_fields[name]  # type: ignore[attr-defined]
        type_ = nested.type
        nullable = nullable or nested.nullable
    return PySparkExpressionRecipe(
        kind="field",
        type=type_,
        nullable=nullable,
        data={
            "scope": schema.__name__,
            "field": ".".join(path),
            "path": path,
        },
    )


def _call(function: str, *args: PySparkExpressionRecipe, **data: object) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("call", args[0].type, args[0].nullable, {"function": function, **data}, args)


def _to_decimal(expression: PySparkExpressionRecipe, *, precision: int, scale: int) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        "call",
        expression.type,
        expression.nullable,
        {"function": "to_decimal", "precision": precision, "scale": scale},
        (expression,),
    )


def _literal(value) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("literal", None, value is None, {"value": value})


def _not_null(expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("is_not_null", None, False, {}, (expression,))


def _is_null(expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("is_null", None, False, {}, (expression,))


def _is_nan(expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("is_nan", structure.Boolean(), False, {}, (expression,))


def _not(expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("not", None, False, {}, (expression,))


def _binary(kind: str, left: PySparkExpressionRecipe, right: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(kind, left.type, False, {}, (left, right))


def _isin(value: PySparkExpressionRecipe, *items: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("isin", structure.Boolean(), True, {}, (value, *items))


def _string_predicate(kind: str, value: PySparkExpressionRecipe, pattern: str) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(kind, structure.Boolean(), value.nullable, {"pattern": pattern}, (value,))


def _item(collection: PySparkExpressionRecipe, key: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("item", structure.String(), True, {}, (collection, key))


def _get_field(parent: PySparkExpressionRecipe, field: str) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("get_field", structure.String(), parent.nullable, {"field": field}, (parent,))


def _cast(value: PySparkExpressionRecipe, spark_type: str) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("cast", structure.Integer(), value.nullable, {"spark_type": spark_type}, (value,))


def _try_cast(value: PySparkExpressionRecipe, spark_type: str) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("try_cast", structure.Integer(), True, {"spark_type": spark_type}, (value,))


def _order(value: PySparkExpressionRecipe, direction: str) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("order", value.type, value.nullable, {"direction": direction}, (value,))


def _when(
    condition: PySparkExpressionRecipe,
    value: PySparkExpressionRecipe,
    fallback: PySparkExpressionRecipe,
) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        "when", value.type, value.nullable or fallback.nullable, {}, (condition, value, fallback)
    )


def _lambda_item() -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("lambda_arg", structure.String(), False, {"name": "item"})


def _lambda_key() -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("lambda_arg", structure.String(), False, {"name": "key"})


def _lambda_value() -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("lambda_arg", structure.String(), True, {"name": "value"})


def _array_transform(array: PySparkExpressionRecipe, body: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        "reserved_v2", array.type, array.nullable, {"function": "array_transform"}, (array, body)
    )


def _array_filter(array: PySparkExpressionRecipe, body: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        "reserved_v2", array.type, array.nullable, {"function": "array_filter"}, (array, body)
    )


def _map_transform_values(mapping: PySparkExpressionRecipe, body: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        "reserved_v2",
        mapping.type,
        mapping.nullable,
        {"function": "map_transform_values"},
        (mapping, _lambda_key(), _lambda_value(), body),
    )


def _map_filter(mapping: PySparkExpressionRecipe, body: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        "reserved_v2",
        mapping.type,
        mapping.nullable,
        {"function": "map_filter"},
        (mapping, _lambda_key(), _lambda_value(), body),
    )


def _window(
    function: str,
    *,
    partition_by: PySparkExpressionRecipe,
    order_by: PySparkExpressionRecipe,
    value: PySparkExpressionRecipe | None = None,
    descending: bool = False,
    preceding: int | None = None,
) -> PySparkExpressionRecipe:
    args = (() if value is None else (value,)) + (order_by, partition_by)
    data = {
        "function": f"window_{function}",
        "descending": descending,
        "offset": 1,
    }
    if preceding is not None:
        data["preceding"] = preceding
    return PySparkExpressionRecipe(
        "reserved_v2",
        value.type if value is not None else structure.Long(),
        value.nullable if value is not None else False,
        data,
        args,
    )


def _hook(
    name: str,
    *,
    lanes: tuple[str, ...],
    outputs: tuple[str, ...],
) -> PySparkHookRecipe:
    return PySparkHookRecipe(
        name=name,
        phase="after",
        target=lanes[0],
        lanes=lanes,
        outputs=outputs,
        sources=lanes,
        schema_mode=structure.SchemaMode.STRICT,
        project_output=False,
        streaming_safe=True,
    )


def _frame(name: str, schema: type[structure.Structure]) -> "FakeFrame":
    return FakeFrame(
        name,
        FakeSchema(
            tuple(
                FakeField(schema_field.column, _type(schema_field.type), schema_field.nullable)
                for schema_field in schema._structure_fields.values()
            )
        ),
    )


def _type(value):
    if value.__class__.__name__ == "String":
        return FakeTypes.StringType()
    if value.__class__.__name__ == "Long":
        return FakeTypes.LongType()
    return FakeTypes.StringType()


def _install_fake_pyspark(monkeypatch, functions: ModuleType) -> None:
    pyspark = ModuleType("pyspark")
    sql = ModuleType("pyspark.sql")
    types = ModuleType("pyspark.sql.types")
    for name in (
        "StructType",
        "StructField",
        "StringType",
        "IntegerType",
        "LongType",
        "FloatType",
        "DoubleType",
        "BooleanType",
        "DateType",
        "TimestampType",
        "DecimalType",
        "ArrayType",
        "MapType",
    ):
        setattr(types, name, getattr(FakeTypes, name))

    setattr(pyspark, "sql", sql)
    setattr(sql, "functions", functions)
    setattr(sql, "types", types)
    setattr(sql, "Window", FakeWindow)
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types)


class FakeInvocation:

    def __init__(self, **inputs) -> None:
        self._structure_bound_inputs = inputs
        self.hook_calls: list[tuple[str, str, object, object]] = []

    def prepare_orders(self, *, orders, spark, ctx):
        self.hook_calls.append(("prepare_orders", orders.name, spark, ctx))
        return orders.with_operation("before-hook")

    def record_published(self, *, published, spark, ctx):
        self.hook_calls.append(("record_published", published.name, spark, ctx))
        return published.with_operation("after-hook")

    def record_ids(self, *, ids, spark, ctx):
        self.hook_calls.append(("record_ids", ids.name, spark, ctx))
        return ids.with_operation("ids-hook")


class FakeFunctions(ModuleType):

    def col(self, name: str):
        return FakeColumn(f"col({name})", source_name=name.rsplit(".", 1)[-1])

    def lit(self, value):
        return FakeColumn(f"lit({value!r})")

    def lower(self, column):
        return FakeColumn(f"lower({column.expression})", source_name=column.source_name)

    def trim(self, column):
        return FakeColumn(f"trim({column.expression})", source_name=column.source_name)

    def upper(self, column):
        return FakeColumn(f"upper({column.expression})", source_name=column.source_name)

    def substring(self, column, start, length):
        return FakeColumn(f"substring({column.expression},{start},{length})")

    def split(self, column, pattern, limit):
        return FakeColumn(f"split({column.expression},{pattern!r},{limit})")

    def regexp_replace(self, column, pattern, replacement):
        return FakeColumn(f"regexp_replace({column.expression},{pattern!r},{replacement!r})")

    def regexp_extract(self, column, pattern, group):
        return FakeColumn(f"regexp_extract({column.expression},{pattern!r},{group})")

    def length(self, column):
        return FakeColumn(f"length({column.expression})")

    def initcap(self, column):
        return FakeColumn(f"initcap({column.expression})")

    def reverse(self, column):
        return FakeColumn(f"reverse({column.expression})")

    def translate(self, column, matching, replacement):
        return FakeColumn(f"translate({column.expression},{matching!r},{replacement!r})")

    def instr(self, column, substring):
        return FakeColumn(f"instr({column.expression},{substring!r})")

    def levenshtein(self, left, right):
        return FakeColumn(f"levenshtein({left.expression},{right.expression})")

    def concat_ws(self, separator, *columns):
        return FakeColumn(f"concat_ws({separator!r},{','.join(column.expression for column in columns)})")

    def date_add(self, column, days):
        return FakeColumn(f"date_add({column.expression},{days})")

    def datediff(self, end, start):
        return FakeColumn(f"datediff({end.expression},{start.expression})")

    def date_trunc(self, unit, column):
        return FakeColumn(f"date_trunc({unit!r},{column.expression})")

    def abs(self, column):
        return FakeColumn(f"abs({column.expression})")

    def round(self, column, scale):
        return FakeColumn(f"round({column.expression},{scale})")

    def ceil(self, column):
        return FakeColumn(f"ceil({column.expression})")

    def floor(self, column):
        return FakeColumn(f"floor({column.expression})")

    def isnan(self, column):
        return FakeColumn(f"isnan({column.expression})")

    def coalesce(self, *columns):
        return FakeColumn("coalesce(" + ",".join(column.expression for column in columns) + ")")

    def struct(self, *columns):
        fields = ",".join(f"{column.output_name or column.expression}={column.expression}" for column in columns)
        return FakeColumn(f"struct({fields})")

    def transform(self, column, function):
        item = FakeColumn("item")
        return FakeColumn(f"transform({column.expression}, lambda item: {function(item).expression})")

    def filter(self, column, function):
        item = FakeColumn("item")
        return FakeColumn(f"filter({column.expression}, lambda item: {function(item).expression})")

    def transform_values(self, column, function):
        key = FakeColumn("key")
        value = FakeColumn("value")
        return FakeColumn(
            f"transform_values({column.expression}, lambda key, value: {function(key, value).expression})"
        )

    def map_filter(self, column, function):
        key = FakeColumn("key")
        value = FakeColumn("value")
        return FakeColumn(f"map_filter({column.expression}, lambda key, value: {function(key, value).expression})")

    def count(self, column):
        return FakeColumn(f"count({column.expression})")

    def sum(self, column):
        return FakeColumn(f"sum({column.expression})")

    def min(self, column):
        return FakeColumn(f"min({column.expression})")

    def max(self, column):
        return FakeColumn(f"max({column.expression})")

    def avg(self, column):
        return FakeColumn(f"avg({column.expression})")

    def countDistinct(self, column):
        return FakeColumn(f"countDistinct({column.expression})")

    def first(self, column, *, ignorenulls: bool):
        return FakeColumn(f"first({column.expression}, ignorenulls={ignorenulls})")

    def when(self, condition, value):
        return FakeWhen(condition, value)

    def broadcast(self, frame):
        return frame.with_operation("broadcast")

    def row_number(self):
        return FakeWindowFunction("row_number")

    def rank(self):
        return FakeWindowFunction("rank")

    def dense_rank(self):
        return FakeWindowFunction("dense_rank")

    def lag(self, column, offset, default=None):
        arguments = f"{column.expression},{offset}"
        if default is not None:
            arguments = f"{arguments},{default!r}"
        return FakeWindowFunction(f"lag({arguments})", call_has_parentheses=False)

    def lead(self, column, offset, default=None):
        arguments = f"{column.expression},{offset}"
        if default is not None:
            arguments = f"{arguments},{default!r}"
        return FakeWindowFunction(f"lead({arguments})", call_has_parentheses=False)

    def monotonically_increasing_id(self):
        return FakeColumn("monotonically_increasing_id()")


@dataclass(frozen=True)
class FakeColumn:
    expression: str
    source_name: str | None = None
    output_name: str | None = None

    def alias(self, name: str):
        return FakeColumn(self.expression, self.source_name, name)

    def cast(self, target: str):
        return FakeColumn(f"cast({self.expression} as {target})", self.source_name)

    def try_cast(self, target: str):
        return FakeColumn(f"try_cast({self.expression} as {target})", self.source_name)

    def over(self, window):
        return FakeColumn(f"{self.expression}.over({window.expression})")

    def isNotNull(self):
        return FakeColumn(f"{self.expression}.isNotNull()")

    def isNull(self):
        return FakeColumn(f"{self.expression}.isNull()")

    def eqNullSafe(self, other):
        return FakeColumn(f"{self.expression}.eqNullSafe({other.expression})")

    def isin(self, *items):
        return FakeColumn(f"{self.expression}.isin({','.join(item.expression for item in items)})")

    def contains(self, value):
        return FakeColumn(f"{self.expression}.contains({value!r})")

    def like(self, pattern):
        return FakeColumn(f"{self.expression}.like({pattern!r})")

    def ilike(self, pattern):
        return FakeColumn(f"{self.expression}.ilike({pattern!r})")

    def rlike(self, pattern):
        return FakeColumn(f"{self.expression}.rlike({pattern!r})")

    def asc_nulls_last(self):
        return FakeColumn(f"{self.expression}.asc_nulls_last()")

    def __getitem__(self, key):
        return FakeColumn(f"{self.expression}[{key!r}]")

    def getField(self, field):
        return FakeColumn(f"{self.expression}.getField({field!r})")

    def __and__(self, other):
        return FakeColumn(f"({self.expression} AND {other.expression})")

    def __or__(self, other):
        return FakeColumn(f"({self.expression} OR {other.expression})")

    def __eq__(self, other):
        return FakeColumn(f"({self.expression} == {other.expression})")

    def __ne__(self, other):
        return FakeColumn(f"({self.expression} != {other.expression})")

    def __gt__(self, other):
        return FakeColumn(f"({self.expression} > {other.expression})")

    def __lt__(self, other):
        return FakeColumn(f"({self.expression} < {other.expression})")

    def __le__(self, other):
        return FakeColumn(f"({self.expression} <= {other.expression})")

    def __ge__(self, other):
        return FakeColumn(f"({self.expression} >= {other.expression})")

    def __add__(self, other):
        return FakeColumn(f"({self.expression} + {other.expression})", self.source_name)

    def __sub__(self, other):
        return FakeColumn(f"({self.expression} - {other.expression})", self.source_name)

    def __mul__(self, other):
        return FakeColumn(f"({self.expression} * {other.expression})", self.source_name)

    def __invert__(self):
        return FakeColumn(f"~({self.expression})")

    def desc(self):
        return FakeColumn(f"{self.expression}.desc()")

    def asc(self):
        return FakeColumn(f"{self.expression}.asc()")


@dataclass(frozen=True)
class FakeWindowFunction:
    name: str
    call_has_parentheses: bool = True

    def over(self, window):
        call = f"{self.name}()" if self.call_has_parentheses else self.name
        return FakeColumn(f"{call}.over({window.expression})")


class FakeWindow:

    @staticmethod
    def partitionBy(*columns):
        return FakeWindowSpec("partitionBy(" + ",".join(column.expression for column in columns) + ")")


@dataclass(frozen=True)
class FakeWindowSpec:
    expression: str

    def orderBy(self, column):
        return FakeWindowSpec(f"{self.expression}.orderBy({column.expression})")

    def rowsBetween(self, start, end):
        return FakeWindowSpec(f"{self.expression}.rowsBetween({start},{end})")


@dataclass(frozen=True)
class FakeWhen:
    condition: FakeColumn
    value: FakeColumn

    def otherwise(self, fallback: FakeColumn):
        return FakeColumn(
            f"when({self.condition.expression}, {self.value.expression}).otherwise({fallback.expression})"
        )


@dataclass(frozen=True)
class FakeFrame:
    name: str
    schema: "FakeSchema"
    operations: tuple[str, ...] = ()

    @property
    def field_names(self) -> list[str]:
        return self.schema.fieldNames()

    def alias(self, name: str):
        return FakeFrame(name, self.schema, self.operations + (f"alias:{name}",))

    def where(self, predicate: FakeColumn):
        return self.with_operation(f"where:{predicate.expression}")

    def join(self, right, predicate: FakeColumn, how: str):
        dedupe = tuple(
            operation
            for operation in right.operations
            if operation.startswith(("withColumn:", "withWatermark:", "drop:", "dropDuplicates"))
            or "__structure_" in operation
        )
        return FakeFrame(
            self.name,
            self.schema,
            self.operations + dedupe + (f"join:{right.name}:{how}:{predicate.expression}",),
        )

    def hint(self, name: str):
        return self.with_operation(f"hint:{name}")

    def select(self, *columns: FakeColumn):
        fields_by_name = {schema_field.name: schema_field for schema_field in self.schema}
        fields = []
        rendered = []
        for column in columns:
            name = column.output_name or column.source_name or column.expression
            source = fields_by_name.get(column.source_name or name)
            fields.append(
                FakeField(
                    name, source.dataType if source else FakeTypes.StringType(), source.nullable if source else True
                )
            )
            rendered.append(f"{name}={column.expression}")
        return FakeFrame(self.name, FakeSchema(tuple(fields)), self.operations + ("select:" + ",".join(rendered),))

    def groupBy(self, *columns: FakeColumn):
        return FakeGroupedFrame(self, columns)

    def with_operation(self, operation: str):
        return FakeFrame(self.name, self.schema, self.operations + (operation,))

    def withColumn(self, name: str, column: FakeColumn):
        return self.with_operation(f"withColumn:{name}={column.expression}")

    def withWatermark(self, field: str, delay: str):
        return self.with_operation(f"withWatermark:{field}:{delay}")

    def drop(self, name: str):
        return self.with_operation(f"drop:{name}")

    def dropDuplicates(self, subset=None):
        suffix = "" if subset is None else ":" + ",".join(subset)
        return self.with_operation(f"dropDuplicates{suffix}")


@dataclass(frozen=True)
class FakeGroupedFrame:
    frame: FakeFrame
    keys: tuple[FakeColumn, ...]

    def agg(self, *columns: FakeColumn):
        fields_by_name = {schema_field.name: schema_field for schema_field in self.frame.schema}
        key_fields = tuple(self._key_field(column, fields_by_name) for column in self.keys)
        aggregate_fields = tuple(
            FakeField(column.output_name or column.expression, self._type(column), True) for column in columns
        )
        group = "groupBy:" + ",".join(
            f"{column.output_name or column.source_name or column.expression}={column.expression}"
            for column in self.keys
        )
        aggregate = "agg:" + ",".join(
            f"{column.output_name or column.expression}={column.expression}" for column in columns
        )
        return FakeFrame(
            self.frame.name,
            FakeSchema((*key_fields, *aggregate_fields)),
            self.frame.operations + (group, aggregate),
        )

    def _type(self, column: FakeColumn):
        if "LongType()" in column.expression:
            return FakeTypes.LongType()
        if "DoubleType()" in column.expression:
            return FakeTypes.DoubleType()
        return FakeTypes.StringType()

    def _key_field(self, column: FakeColumn, fields_by_name: dict[str, "FakeField"]) -> "FakeField":
        name = column.output_name or column.source_name or column.expression
        source = fields_by_name.get(column.source_name or "")
        return FakeField(
            name,
            source.dataType if source else FakeTypes.StringType(),
            source.nullable if source else True,
        )


@dataclass(frozen=True)
class FakeSchema:
    fields: tuple["FakeField", ...]

    def fieldNames(self) -> list[str]:
        return [schema_field.name for schema_field in self.fields]

    def __iter__(self):
        return iter(self.fields)

    def __getitem__(self, name: str):
        for schema_field in self.fields:
            if schema_field.name == name:
                return schema_field
        raise KeyError(name)


@dataclass(frozen=True)
class FakeField:
    name: str
    dataType: "FakeType | FakeSchema"
    nullable: bool


@dataclass(frozen=True)
class FakeType:
    name: str
    args: tuple = ()

    def __str__(self) -> str:
        if not self.args:
            return f"{self.name}()"
        return f"{self.name}({', '.join(str(arg) for arg in self.args)})"


class FakeTypes:

    @staticmethod
    def StructType(fields):
        return FakeSchema(tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return FakeField(name, dataType, nullable)

    @staticmethod
    def StringType():
        return FakeType("StringType")

    @staticmethod
    def IntegerType():
        return FakeType("IntegerType")

    @staticmethod
    def LongType():
        return FakeType("LongType")

    @staticmethod
    def FloatType():
        return FakeType("FloatType")

    @staticmethod
    def DoubleType():
        return FakeType("DoubleType")

    @staticmethod
    def BooleanType():
        return FakeType("BooleanType")

    @staticmethod
    def DateType():
        return FakeType("DateType")

    @staticmethod
    def TimestampType():
        return FakeType("TimestampType")

    @staticmethod
    def DecimalType(precision, scale):
        return FakeType("DecimalType", (precision, scale))

    @staticmethod
    def ArrayType(element, *, containsNull):
        return FakeType("ArrayType", (element, containsNull))

    @staticmethod
    def MapType(key, value, *, valueContainsNull):
        return FakeType("MapType", (key, value, valueContainsNull))
