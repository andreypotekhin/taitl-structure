from structure import Long, String, Structure, Transform, count, field, group_by, input, output, sum, transform
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class RawOrder(Structure):
    customer_id = field(String(), nullable=False)
    quantity = field(Long(), nullable=False)


class CustomerTotal(Structure):
    customer_id = field(String(), nullable=False)
    order_count = field(Long(), nullable=False)
    quantity = field(Long(), nullable=False)


@transform
class CustomerTotals(Transform):
    rows = input(RawOrder)
    totals = output(CustomerTotal)

    def summarize(self, row: RawOrder) -> CustomerTotal:
        return group_by(row.customer_id).agg(order_count=count(), quantity=sum(row.quantity)).as_schema(CustomerTotal)


def test_grouped_aggregate_step_renders_spark_visible_group_by() -> None:
    plan = PySpark.plan.lower()(compile_transform(CustomerTotals))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert ".groupBy(" in text
    assert 'F.col("raw_order.customer_id").alias("customer_id")' in text
    assert "F.count(F.lit(1)).cast(T.LongType()).alias(\"order_count\")" in text
    assert 'F.sum(F.col("raw_order.quantity")).cast(T.LongType()).alias("quantity")' in text
    assert 'F.col("customer_id")' in text
