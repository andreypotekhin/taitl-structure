import pytest

from structure import *
from structure.core.cli.api import CliApp
from structure.core.compiler.api import Compiler
from structure.core.dsl.model.expr.expressions import literal
from structure.core.target.pyspark.api import PySpark
from structure.core.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class RawOrder(Schema):
    customer_id = field.string(nullable=False)
    quantity = field.long(nullable=False)


class CustomerTotal(Schema):
    customer_id = field.string(nullable=False)
    order_count = field.long(nullable=False)
    distinct_customers = field.long(nullable=False)
    quantity = field.long(nullable=False)
    min_quantity = field.long(nullable=False)
    max_quantity = field.long(nullable=False)
    avg_quantity = field.double(nullable=False)


class AdvancedCustomerTotal(Schema):
    customer_id = field.string(nullable=True)
    paid_quantity = field.long(nullable=True)
    any_large = field.boolean(nullable=True)
    quantity_stddev = field.double(nullable=True)
    exact_quantity_percentile = field.double(nullable=True)
    quantity_skewness = field.double(nullable=True)
    quantity_kurtosis = field.double(nullable=True)
    approximate_customers = field.long(nullable=False)
    ordered_first_customer = field.string(nullable=True)
    ordered_last_customer = field.string(nullable=True)
    customers = field.array(field.string(), contains_null=False, nullable=True)


class RawSale(Schema):
    region = field.string(nullable=False)
    customer_id = field.string(nullable=False)
    quantity = field.long(nullable=False)


class GroupingSetTotal(Schema):
    region = field.string(nullable=True)
    customer_id = field.string(nullable=True)
    order_count = field.long(nullable=False)
    grouping_id = field.integer(nullable=False)
    region_grouped = field.boolean(nullable=False)
    customer_grouped = field.boolean(nullable=False)


class GrandTotal(Schema):
    order_count = field.long(nullable=False)
    grouping_id = field.integer(nullable=False)


class GrandValueTotal(Schema):
    total_quantity = field.long(nullable=False)
    average_quantity = field.double(nullable=False)
    minimum_quantity = field.long(nullable=False)
    maximum_quantity = field.long(nullable=False)
    first_customer = field.string(nullable=False)
    last_customer = field.string(nullable=False)


@transform
class CustomerTotals(Transform):
    rows = input(RawOrder)
    totals = output(CustomerTotal)

    def summarize(self, row: RawOrder) -> CustomerTotal:
        group_by(row.customer_id)
        return CustomerTotal(
            customer_id=row.customer_id,
            order_count=count(),
            distinct_customers=count_distinct(row.customer_id),
            quantity=sum(row.quantity),
            min_quantity=min(row.quantity),
            max_quantity=max(row.quantity),
            avg_quantity=avg(row.quantity),
        )


@transform
class AdvancedCustomerTotals(Transform):
    rows = input(RawOrder)
    totals = output(AdvancedCustomerTotal)

    def summarize(self, row: RawOrder) -> AdvancedCustomerTotal:
        rollup(customer_id=row.customer_id)
        return AdvancedCustomerTotal(
            customer_id=row.customer_id,
            paid_quantity=sum(row.quantity, where=literal(row.quantity) > 0),
            any_large=bool_or(literal(row.quantity) > 10),
            quantity_stddev=stddev(row.quantity),
            exact_quantity_percentile=percentile(row.quantity, 0.5),
            quantity_skewness=skewness(row.quantity),
            quantity_kurtosis=kurtosis(row.quantity),
            approximate_customers=approx_count_distinct(row.customer_id),
            ordered_first_customer=first_value(row.customer_id, order_by=row.quantity, where=row.quantity > 0),
            ordered_last_customer=last_value(row.customer_id, order_by=row.quantity, where=row.quantity > 0),
            customers=collect_set(row.customer_id, element_type=types.string()),
        )


@transform
class SaleGroupingSets(Transform):
    rows = input(RawSale)
    totals = output(GroupingSetTotal)

    def summarize(self, row: RawSale) -> GroupingSetTotal:
        grouping_sets((row.region, row.customer_id), (row.region,), ()).having(lambda total: total.order_count > 0)
        return GroupingSetTotal(
            region=row.region,
            customer_id=row.customer_id,
            order_count=count(),
            grouping_id=grouping_id(),
            region_grouped=is_grouped(row.region),
            customer_grouped=is_grouped(row.customer_id),
        )


@transform
class SaleGrandTotal(Transform):
    rows = input(RawSale)
    totals = output(GrandTotal)

    def summarize(self, row: RawSale) -> GrandTotal:
        grouping_sets(())
        return GrandTotal(order_count=count(), grouping_id=grouping_id())


@transform
class SaleGrandValueTotal(Transform):
    rows = input(RawSale)
    totals = output(GrandValueTotal)

    def summarize(self, row: RawSale) -> GrandValueTotal:
        grouping_sets(())
        return GrandValueTotal(
            total_quantity=sum(row.quantity),
            average_quantity=avg(row.quantity),
            minimum_quantity=min(row.quantity),
            maximum_quantity=max(row.quantity),
            first_customer=first_value(row.customer_id, order_by=row.quantity),
            last_customer=last_value(row.customer_id, order_by=row.quantity),
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
        "metrics=count,count_distinct,sum,min,max,avg streaming_modes=append|update)"
    ) in report


def test_advanced_aggregate_helpers_render_spark_visible_rollup_and_metrics() -> None:
    plan = PySpark.plan.lower()(compile_transform(AdvancedCustomerTotals))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert ".rollup(" in text
    assert 'F.sum(F.when((F.col("raw_order.quantity") > F.lit(0)), F.col("raw_order.quantity")))' in text
    assert 'F.bool_or((F.col("raw_order.quantity") > F.lit(10))).cast(T.BooleanType()).alias("any_large")' in text
    assert 'F.stddev(F.col("raw_order.quantity")).cast(T.DoubleType()).alias("quantity_stddev")' in text
    assert (
        'F.percentile(F.col("raw_order.quantity"), 0.5, 1).cast(T.DoubleType()).alias("exact_quantity_percentile")'
        in text
    )
    assert 'F.skewness(F.col("raw_order.quantity")).cast(T.DoubleType()).alias("quantity_skewness")' in text
    assert 'F.kurtosis(F.col("raw_order.quantity")).cast(T.DoubleType()).alias("quantity_kurtosis")' in text
    assert 'F.approx_count_distinct(F.col("raw_order.customer_id")).cast(T.LongType())' in text
    assert (
        'F.min_by(F.col("raw_order.customer_id"), F.when((F.col("raw_order.quantity") > F.lit(0)), '
        'F.col("raw_order.quantity"))).alias("ordered_first_customer")' in text
    )
    assert (
        'F.max_by(F.col("raw_order.customer_id"), F.when((F.col("raw_order.quantity") > F.lit(0)), '
        'F.col("raw_order.quantity"))).alias("ordered_last_customer")' in text
    )
    assert 'F.collect_set(F.col("raw_order.customer_id")).cast(T.ArrayType(T.StringType(), containsNull=False))' in text


def test_grouping_sets_lower_to_explicit_levels_and_render_union_branches() -> None:
    plan = PySpark.plan.lower()(compile_transform(SaleGroupingSets))

    aggregate = plan.steps[0].aggregate
    assert aggregate is not None
    assert aggregate.grouping == "grouping_sets"
    assert aggregate.levels == (("region", "customer_id"), ("region",), ())

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert 'withColumn("__structure_group_0_region", F.col("raw_sale.region"))' in text
    assert 'withColumn("__structure_group_1_customer_id", F.col("raw_sale.customer_id"))' in text
    assert 'rows_grouping_set_1 = rows.groupBy(' in text
    assert 'rows_grouping_set_2 = rows.groupBy(' in text
    assert 'rows_grouping_set_3 = rows.groupBy(' in text
    assert 'F.lit(None).cast(T.StringType()).alias("customer_id")' in text
    assert 'F.lit(None).cast(T.StringType()).alias("region")' in text
    assert 'F.lit(0).cast(T.IntegerType()).alias("grouping_id")' in text
    assert 'F.lit(1).cast(T.IntegerType()).alias("grouping_id")' in text
    assert 'F.lit(3).cast(T.IntegerType()).alias("grouping_id")' in text
    assert "rows = rows.unionByName(rows_grouping_set_2)" in text
    assert "rows = rows.unionByName(rows_grouping_set_3)" in text
    assert 'rows = rows.where((F.col("order_count") > F.lit(0)))' in text


def test_grouping_sets_supports_a_global_aggregate_level() -> None:
    plan = PySpark.plan.lower()(compile_transform(SaleGrandTotal))

    aggregate = plan.steps[0].aggregate
    assert aggregate is not None
    assert aggregate.keys == ()
    assert aggregate.levels == ((),)

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert "rows_grouping_set_1 = rows.groupBy(" in text
    assert 'F.lit(0).cast(T.IntegerType()).alias("grouping_id")' in text


def test_global_grouping_set_marks_value_aggregates_nullable_for_empty_input() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(SaleGrandValueTotal)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"
    assert "GrandValueTotal.total_quantity" in raised.value.diagnostic.problem


def test_grouping_sets_traceability_and_explain_name_levels() -> None:
    recipe = PySpark.plan.lower()(compile_transform(SaleGroupingSets))
    traceability = Compiler.traceability.build()(
        recipe,
        source_transform="tests.SaleGroupingSets",
        transform_module="tests.SaleGroupingSetsGenerated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert dependencies["GroupingSetTotal.region"].detail["function"] == "key"
    assert dependencies["GroupingSetTotal.order_count"].detail["function"] == "count"
    assert dependencies["GroupingSetTotal.having"].operation == "having"
    assert dependencies["GroupingSetTotal.having"].sources == ("GroupingSetTotal.order_count",)

    report = CliApp.render_explain_report()(SaleGroupingSets)

    assert "aggregate(aggregate keys=region,customer_id levels=region+customer_id|region|()" in report
    assert "having=1" in report


def test_grouping_sets_reject_non_nullable_omitted_key_fields() -> None:
    class BadTotal(Schema):
        region = field.string(nullable=False)
        customer_id = field.string(nullable=False)
        order_count = field.long(nullable=False)

    @transform
    class BadGroupingSets(Transform):
        rows = input(RawSale)
        totals = output(BadTotal)

        def summarize(self, row: RawSale) -> BadTotal:
            grouping_sets((row.region, row.customer_id), (row.region,), ())
            return BadTotal(region=row.region, customer_id=row.customer_id, order_count=count())

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadGroupingSets)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert "grouping_sets(...)" in diagnostic.problem


def test_having_rejects_pre_aggregate_input_field_reads() -> None:
    class Total(Schema):
        customer_id = field.string(nullable=False)
        order_count = field.long(nullable=False)

    @transform
    class BadHaving(Transform):
        rows = input(RawSale)
        totals = output(Total)

        def summarize(self, row: RawSale) -> Total:
            group_by(customer_id=row.customer_id).having(lambda total: literal(row.quantity) > 0)
            return Total(customer_id=row.customer_id, order_count=count())

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadHaving)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert "having(...)" in diagnostic.problem
    assert "rows" in diagnostic.problem


def test_having_rejects_incompatible_comparison_operands() -> None:
    class Total(Schema):
        customer_id = field.string(nullable=False)
        order_count = field.long(nullable=False)

    @transform
    class BadHaving(Transform):
        rows = input(RawSale)
        totals = output(Total)

        def summarize(self, row: RawSale) -> Total:
            group_by(customer_id=row.customer_id).having(lambda total: total.order_count == "one")
            return Total(customer_id=row.customer_id, order_count=count())

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadHaving)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert "compatible Structure expression types" in raised.value.diagnostic.problem


def test_aggregate_filter_rejects_incompatible_comparison_operands() -> None:
    class Total(Schema):
        customer_id = field.string(nullable=False)
        order_count = field.long(nullable=True)

    @transform
    class BadAggregateFilter(Transform):
        rows = input(RawSale)
        totals = output(Total)

        def summarize(self, row: RawSale) -> Total:
            group_by(customer_id=row.customer_id)
            return Total(customer_id=row.customer_id, order_count=count(where=row.quantity == "one"))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadAggregateFilter)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert "compatible Structure expression types" in raised.value.diagnostic.problem


def test_statement_having_binds_to_aggregate_output_scope() -> None:
    class Total(Schema):
        customer_id = field.string(nullable=False)
        order_count = field.long(nullable=False)

    @transform
    class StatementHaving(Transform):
        rows = input(RawSale)
        totals = output(Total)

        def summarize(self, row: RawSale) -> Total:
            group_by(customer_id=row.customer_id)
            having(lambda total: total.order_count > 0)
            return Total(customer_id=row.customer_id, order_count=count())

    plan = PySpark.plan.lower()(compile_transform(StatementHaving))

    assert plan.steps[0].aggregate is not None
    assert plan.steps[0].aggregate.having is not None


@pytest.mark.parametrize("grouping", ("group_by", "rollup", "cube", "grouping_sets"))
def test_each_grouping_form_accepts_chained_having(grouping: str) -> None:
    class Total(Schema):
        customer_id = field.string(nullable=True)
        order_count = field.long(nullable=False)

    @transform
    class ChainedHaving(Transform):
        rows = input(RawSale)
        totals = output(Total)

        def summarize(self, row: RawSale) -> Total:
            if grouping == "group_by":
                group_by(customer_id=row.customer_id).having(lambda total: total.order_count > 0)
            elif grouping == "rollup":
                rollup(customer_id=row.customer_id).having(lambda total: total.order_count > 0)
            elif grouping == "cube":
                cube(customer_id=row.customer_id).having(lambda total: total.order_count > 0)
            else:
                grouping_sets((row.customer_id,), ()).having(lambda total: total.order_count > 0)
            return Total(customer_id=row.customer_id, order_count=count())

    plan = PySpark.plan.lower()(compile_transform(ChainedHaving))

    assert plan.steps[0].aggregate is not None
    assert plan.steps[0].aggregate.grouping == grouping
    assert plan.steps[0].aggregate.having is not None


def test_bare_and_chained_having_lower_identically() -> None:
    class Total(Schema):
        customer_id = field.string(nullable=False)
        order_count = field.long(nullable=False)

    @transform
    class BareHaving(Transform):
        rows = input(RawSale)
        totals = output(Total)

        def summarize(self, row: RawSale) -> Total:
            group_by(customer_id=row.customer_id)
            having(lambda total: total.order_count > 0)
            return Total(customer_id=row.customer_id, order_count=count())

    @transform
    class ChainedHaving(Transform):
        rows = input(RawSale)
        totals = output(Total)

        def summarize(self, row: RawSale) -> Total:
            group_by(customer_id=row.customer_id).having(lambda total: total.order_count > 0)
            return Total(customer_id=row.customer_id, order_count=count())

    bare = PySpark.plan.lower()(compile_transform(BareHaving)).steps[0].aggregate
    chained = PySpark.plan.lower()(compile_transform(ChainedHaving)).steps[0].aggregate

    assert bare == chained
