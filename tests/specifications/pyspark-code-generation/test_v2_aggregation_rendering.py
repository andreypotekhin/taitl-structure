import structure
from structure.app.cli.api import CliApp
from structure.app.compiler.api import Compiler
from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.expr.expressions import literal
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class RawOrder(structure.Structure):
    customer_id = structure.field(structure.String(), nullable=False)
    quantity = structure.field(structure.Long(), nullable=False)


class CustomerTotal(structure.Structure):
    customer_id = structure.field(structure.String(), nullable=False)
    order_count = structure.field(structure.Long(), nullable=False)
    distinct_customers = structure.field(structure.Long(), nullable=False)
    quantity = structure.field(structure.Long(), nullable=False)
    min_quantity = structure.field(structure.Long(), nullable=False)
    max_quantity = structure.field(structure.Long(), nullable=False)
    avg_quantity = structure.field(structure.Double(), nullable=False)


class AdvancedCustomerTotal(structure.Structure):
    customer_id = structure.field(structure.String(), nullable=True)
    paid_quantity = structure.field(structure.Long(), nullable=True)
    any_large = structure.field(structure.Boolean(), nullable=True)
    quantity_stddev = structure.field(structure.Double(), nullable=True)
    approximate_customers = structure.field(structure.Long(), nullable=False)
    ordered_first_customer = structure.field(structure.String(), nullable=False)
    ordered_last_customer = structure.field(structure.String(), nullable=False)
    customers = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


@structure.transform
class CustomerTotals(structure.Transform):
    rows = structure.input(RawOrder)
    totals = structure.output(CustomerTotal)

    def summarize(self, row: RawOrder) -> CustomerTotal:
        return (
            structure.group_by(row.customer_id)
            .agg(
                order_count=structure.count(),
                distinct_customers=structure.count_distinct(row.customer_id),
                quantity=structure.sum(row.quantity),
                min_quantity=structure.min(row.quantity),
                max_quantity=structure.max(row.quantity),
                avg_quantity=structure.avg(row.quantity),
            )
            .as_schema(CustomerTotal)
        )


@structure.transform
class AdvancedCustomerTotals(structure.Transform):
    rows = structure.input(RawOrder)
    totals = structure.output(AdvancedCustomerTotal)

    def summarize(self, row: RawOrder) -> AdvancedCustomerTotal:
        return (
            structure.rollup(customer_id=row.customer_id)
            .agg(
                paid_quantity=structure.sum(row.quantity, where=literal(row.quantity) > 0),
                any_large=structure.bool_or(literal(row.quantity) > 10),
                quantity_stddev=structure.stddev(row.quantity),
                approximate_customers=structure.approx_count_distinct(row.customer_id),
                ordered_first_customer=structure.first_value(row.customer_id, order_by=row.quantity),
                ordered_last_customer=structure.last_value(row.customer_id, order_by=row.quantity),
                customers=structure.collect_set(row.customer_id, element_type=structure.String()),
            )
            .as_schema(AdvancedCustomerTotal)
        )


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
        "metrics=count,count_distinct,sum,min,max,avg streaming_modes=update|complete)"
    ) in report


def test_advanced_aggregate_helpers_render_spark_visible_rollup_and_metrics() -> None:
    plan = PySpark.plan.lower()(compile_transform(AdvancedCustomerTotals))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert ".rollup(" in text
    assert 'F.sum(F.when((F.col("raw_order.quantity") > F.lit(0)), F.col("raw_order.quantity")))' in text
    assert 'F.bool_or((F.col("raw_order.quantity") > F.lit(10))).cast(T.BooleanType()).alias("any_large")' in text
    assert 'F.stddev(F.col("raw_order.quantity")).cast(T.DoubleType()).alias("quantity_stddev")' in text
    assert 'F.approx_count_distinct(F.col("raw_order.customer_id")).cast(T.LongType())' in text
    assert (
        'F.min_by(F.col("raw_order.customer_id"), F.col("raw_order.quantity")).alias("ordered_first_customer")' in text
    )
    assert (
        'F.max_by(F.col("raw_order.customer_id"), F.col("raw_order.quantity")).alias("ordered_last_customer")' in text
    )
    assert 'F.collect_set(F.col("raw_order.customer_id")).cast(T.ArrayType(T.StringType(), containsNull=False))' in text
