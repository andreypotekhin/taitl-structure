import pytest

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


class RawSale(structure.Structure):
    region = structure.field(structure.String(), nullable=False)
    customer_id = structure.field(structure.String(), nullable=False)
    quantity = structure.field(structure.Long(), nullable=False)


class GroupingSetTotal(structure.Structure):
    region = structure.field(structure.String(), nullable=True)
    customer_id = structure.field(structure.String(), nullable=True)
    order_count = structure.field(structure.Long(), nullable=False)
    grouping_id = structure.field(structure.Integer(), nullable=False)
    region_grouped = structure.field(structure.Boolean(), nullable=False)
    customer_grouped = structure.field(structure.Boolean(), nullable=False)


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


@structure.transform
class SaleGroupingSets(structure.Transform):
    rows = structure.input(RawSale)
    totals = structure.output(GroupingSetTotal)

    def summarize(self, row: RawSale) -> GroupingSetTotal:
        return (
            structure.grouping_sets((row.region, row.customer_id), (row.region,), ())
            .agg(
                order_count=structure.count(),
                grouping_id=structure.grouping_id(),
                region_grouped=structure.is_grouped(row.region),
                customer_grouped=structure.is_grouped(row.customer_id),
            )
            .having(lambda total: total.order_count > 0)
            .as_schema(GroupingSetTotal)
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
    class BadTotal(structure.Structure):
        region = structure.field(structure.String(), nullable=False)
        customer_id = structure.field(structure.String(), nullable=False)
        order_count = structure.field(structure.Long(), nullable=False)

    @structure.transform
    class BadGroupingSets(structure.Transform):
        rows = structure.input(RawSale)
        totals = structure.output(BadTotal)

        def summarize(self, row: RawSale) -> BadTotal:
            return (
                structure.grouping_sets((row.region, row.customer_id), (row.region,), ())
                .agg(order_count=structure.count())
                .as_schema(BadTotal)
            )

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadGroupingSets)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert "grouping_sets(...)" in diagnostic.problem


def test_having_rejects_pre_aggregate_input_field_reads() -> None:
    class Total(structure.Structure):
        customer_id = structure.field(structure.String(), nullable=False)
        order_count = structure.field(structure.Long(), nullable=False)

    @structure.transform
    class BadHaving(structure.Transform):
        rows = structure.input(RawSale)
        totals = structure.output(Total)

        def summarize(self, row: RawSale) -> Total:
            return (
                structure.group_by(customer_id=row.customer_id)
                .agg(order_count=structure.count())
                .having(lambda total: literal(row.quantity) > 0)
                .as_schema(Total)
            )

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadHaving)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert "having(...)" in diagnostic.problem
    assert "rows" in diagnostic.problem


def test_statement_having_binds_to_aggregate_output_scope() -> None:
    class Total(structure.Structure):
        customer_id = structure.field(structure.String(), nullable=False)
        order_count = structure.field(structure.Long(), nullable=False)

    @structure.transform
    class StatementHaving(structure.Transform):
        rows = structure.input(RawSale)
        totals = structure.output(Total)

        def summarize(self, row: RawSale) -> Total:
            structure.group_by(customer_id=row.customer_id)
            structure.having(lambda total: total.order_count > 0)
            return Total(customer_id=row.customer_id, order_count=structure.count())

    plan = PySpark.plan.lower()(compile_transform(StatementHaving))

    assert plan.steps[0].aggregate is not None
    assert plan.steps[0].aggregate.having is not None
