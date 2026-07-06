from structure import (
    Array,
    Boolean,
    Double,
    Long,
    String,
    Structure,
    Transform,
    approx_count_distinct,
    avg,
    bool_or,
    collect_set,
    count,
    count_distinct,
    field,
    first_value,
    group_by,
    input,
    last_value,
    max,
    min,
    output,
    rollup,
    stddev,
    sum,
    transform,
)
from structure.app.cli.api import CliApp
from structure.app.compiler.api import Compiler
from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.expr.expressions import literal
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class RawOrder(Structure):
    customer_id = field(String(), nullable=False)
    quantity = field(Long(), nullable=False)


class CustomerTotal(Structure):
    customer_id = field(String(), nullable=False)
    order_count = field(Long(), nullable=False)
    distinct_customers = field(Long(), nullable=False)
    quantity = field(Long(), nullable=False)
    min_quantity = field(Long(), nullable=False)
    max_quantity = field(Long(), nullable=False)
    avg_quantity = field(Double(), nullable=False)


class AdvancedCustomerTotal(Structure):
    customer_id = field(String(), nullable=True)
    paid_quantity = field(Long(), nullable=True)
    any_large = field(Boolean(), nullable=True)
    quantity_stddev = field(Double(), nullable=True)
    approximate_customers = field(Long(), nullable=False)
    ordered_first_customer = field(String(), nullable=False)
    ordered_last_customer = field(String(), nullable=False)
    customers = field(Array(String(), contains_null=False), nullable=True)


@transform
class CustomerTotals(Transform):
    rows = input(RawOrder)
    totals = output(CustomerTotal)

    def summarize(self, row: RawOrder) -> CustomerTotal:
        return group_by(row.customer_id).agg(
            order_count=count(),
            distinct_customers=count_distinct(row.customer_id),
            quantity=sum(row.quantity),
            min_quantity=min(row.quantity),
            max_quantity=max(row.quantity),
            avg_quantity=avg(row.quantity),
        ).as_schema(CustomerTotal)


@transform
class AdvancedCustomerTotals(Transform):
    rows = input(RawOrder)
    totals = output(AdvancedCustomerTotal)

    def summarize(self, row: RawOrder) -> AdvancedCustomerTotal:
        return rollup(customer_id=row.customer_id).agg(
            paid_quantity=sum(row.quantity, where=literal(row.quantity) > 0),
            any_large=bool_or(literal(row.quantity) > 10),
            quantity_stddev=stddev(row.quantity),
            approximate_customers=approx_count_distinct(row.customer_id),
            ordered_first_customer=first_value(row.customer_id, order_by=row.quantity),
            ordered_last_customer=last_value(row.customer_id, order_by=row.quantity),
            customers=collect_set(row.customer_id, element_type=String()),
        ).as_schema(AdvancedCustomerTotal)


def test_grouped_aggregate_step_renders_spark_visible_group_by() -> None:
    plan = PySpark.plan.lower()(compile_transform(CustomerTotals))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert ".groupBy(" in text
    assert 'F.col("raw_order.customer_id").alias("customer_id")' in text
    assert "F.count(F.lit(1)).cast(T.LongType()).alias(\"order_count\")" in text
    assert 'F.countDistinct(F.col("raw_order.customer_id")).cast(T.LongType()).alias("distinct_customers")' in text
    assert 'F.sum(F.col("raw_order.quantity")).cast(T.LongType()).alias("quantity")' in text
    assert 'F.min(F.col("raw_order.quantity")).cast(T.LongType()).alias("min_quantity")' in text
    assert 'F.max(F.col("raw_order.quantity")).cast(T.LongType()).alias("max_quantity")' in text
    assert 'F.avg(F.col("raw_order.quantity")).cast(T.DoubleType()).alias("avg_quantity")' in text
    assert 'F.col("customer_id")' in text


def test_grouped_aggregate_traceability_records_static_dataflow() -> None:
    recipe = PySpark.plan.lower()(compile_transform(CustomerTotals))
    traceability = Compiler.traceability.build()(
        recipe,
        source_transform="tests.CustomerTotals",
        transform_module="tests.CustomerTotalsGenerated",
    )

    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert dependencies["CustomerTotal.customer_id"].operation == "aggregate"
    assert dependencies["CustomerTotal.customer_id"].detail["function"] == "key"
    assert dependencies["CustomerTotal.quantity"].sources == ("rows.quantity",)
    assert dependencies["CustomerTotal.quantity"].detail["function"] == "sum"
    assert dependencies["CustomerTotal.order_count"].sources == ("rows",)
    assert dependencies["CustomerTotal.order_count"].detail["function"] == "count"
    assert any(record.ir.endswith(".aggregate.quantity") for record in traceability.provenance)


def test_grouped_aggregate_explain_names_keys_and_metrics() -> None:
    report = CliApp.render_explain_report()(CustomerTotals)

    assert (
        "operations: aggregate(aggregate keys=customer_id "
        "metrics=count,count_distinct,sum,min,max,avg)"
    ) in report


def test_advanced_aggregate_helpers_render_spark_visible_rollup_and_metrics() -> None:
    plan = PySpark.plan.lower()(compile_transform(AdvancedCustomerTotals))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert ".rollup(" in text
    assert 'F.sum(F.when((F.col("raw_order.quantity") > F.lit(0)), F.col("raw_order.quantity")))' in text
    assert 'F.bool_or((F.col("raw_order.quantity") > F.lit(10))).cast(T.BooleanType()).alias("any_large")' in text
    assert 'F.stddev(F.col("raw_order.quantity")).cast(T.DoubleType()).alias("quantity_stddev")' in text
    assert 'F.approx_count_distinct(F.col("raw_order.customer_id")).cast(T.LongType())' in text
    assert 'F.min_by(F.col("raw_order.customer_id"), F.col("raw_order.quantity")).alias("ordered_first_customer")' in text
    assert 'F.max_by(F.col("raw_order.customer_id"), F.col("raw_order.quantity")).alias("ordered_last_customer")' in text
    assert 'F.collect_set(F.col("raw_order.customer_id")).cast(T.ArrayType(T.StringType(), containsNull=False))' in text
