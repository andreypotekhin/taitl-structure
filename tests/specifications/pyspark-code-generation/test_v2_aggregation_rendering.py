from structure import (
    Double,
    Long,
    String,
    Structure,
    Transform,
    avg,
    count,
    count_distinct,
    field,
    group_by,
    input,
    max,
    min,
    output,
    sum,
    transform,
)
from structure.app.cli.api import CliApp
from structure.app.compiler.api import Compiler
from structure.app.dsl.api import compile_transform
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
