from typing import Any, cast

import structure
from structure import step as dsl_step
from structure import sum, temporal_one, transform, trim, where, window, window_count, window_sum
from structure.app.compiler.api import Compiler
from structure.app.dsl.api import compile_transform
from structure.app.target.capabilities.api import Capabilities
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkRuntimeModule import render_pyspark_runtime_module
from structure.app.target.pyspark.commands.RenderPySparkTransformModule import render_pyspark_transform_module

CLASSIC_ONLY_TOKENS = (
    "SparkContext",
    "sparkContext",
    "SQLContext",
    "sql_ctx",
    "_jdf",
    "_jvm",
    ".rdd",
    ".collect(",
    ".toPandas(",
    "foreachPartition",
    "mapInPandas",
)


class RawBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    promo_code = structure.field(structure.String(), nullable=True)
    event_date = structure.field(structure.Date(), nullable=False)
    sequence = structure.field(structure.Long(), nullable=False)
    amount = structure.field(structure.Long(), nullable=False)
    tags = structure.field(structure.Array(structure.String(), contains_null=True), nullable=True)
    attributes = structure.field(
        structure.Map(structure.String(), structure.String(), value_contains_null=True), nullable=True
    )


class RankedBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    promo_code = structure.field(structure.String(), nullable=True)
    event_date = structure.field(structure.Date(), nullable=False)
    sequence = structure.field(structure.Long(), nullable=False)
    amount = structure.field(structure.Long(), nullable=False)
    tags = structure.field(structure.Array(structure.String(), contains_null=True), nullable=True)
    attributes = structure.field(
        structure.Map(structure.String(), structure.String(), value_contains_null=True), nullable=True
    )
    row_number = structure.field(structure.Long(), nullable=False)
    rank = structure.field(structure.Long(), nullable=False)
    dense_rank = structure.field(structure.Long(), nullable=False)
    previous_sequence = structure.field(structure.Long(), nullable=True)
    next_sequence = structure.field(structure.Long(), nullable=True)
    rolling_units = structure.field(structure.Long(), nullable=False)
    rolling_avg_units = structure.field(structure.Double(), nullable=False)
    rolling_min_units = structure.field(structure.Long(), nullable=False)
    rolling_max_units = structure.field(structure.Long(), nullable=False)


class AccountSummary(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_count = structure.field(structure.Long(), nullable=False)
    distinct_events = structure.field(structure.Long(), nullable=False)
    total_amount = structure.field(structure.Long(), nullable=False)
    min_amount = structure.field(structure.Long(), nullable=False)
    max_amount = structure.field(structure.Long(), nullable=False)
    avg_amount = structure.field(structure.Double(), nullable=False)


class Customer(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    name = structure.field(structure.String(), nullable=True)


class Product(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    name = structure.field(structure.String(), nullable=True)
    ingested_at = structure.field(structure.Long(), nullable=False)


class BlockedProduct(structure.Schema):
    id = structure.field(structure.String(), nullable=False)


class Promotion(structure.Schema):
    code = structure.field(structure.String(), nullable=False)
    name = structure.field(structure.String(), nullable=True)
    valid_from = structure.field(structure.Date(), nullable=False)
    valid_to = structure.field(structure.Date(), nullable=True)


class Shipment(structure.Schema):
    event_id = structure.field(structure.String(), nullable=False)
    line = structure.field(structure.Long(), nullable=False)


class CustomerBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    promo_code = structure.field(structure.String(), nullable=True)
    event_date = structure.field(structure.Date(), nullable=False)
    customer_name = structure.field(structure.String(), nullable=True)


class ProductBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    promo_code = structure.field(structure.String(), nullable=True)
    event_date = structure.field(structure.Date(), nullable=False)
    customer_name = structure.field(structure.String(), nullable=True)
    product_name = structure.field(structure.String(), nullable=True)


class PromotedBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    customer_name = structure.field(structure.String(), nullable=True)
    product_name = structure.field(structure.String(), nullable=True)
    promotion_name = structure.field(structure.String(), nullable=True)


class JoinedBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    customer_name = structure.field(structure.String(), nullable=True)
    product_name = structure.field(structure.String(), nullable=True)
    promotion_name = structure.field(structure.String(), nullable=True)
    shipment_line = structure.field(structure.Long(), nullable=False)


class RowsetMatchBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=True)
    event_id = structure.field(structure.String(), nullable=True)
    customer_id = structure.field(structure.String(), nullable=True)
    customer_name = structure.field(structure.String(), nullable=True)


class RowsetBackfillBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=True)
    event_id = structure.field(structure.String(), nullable=True)
    customer_id = structure.field(structure.String(), nullable=False)
    customer_name = structure.field(structure.String(), nullable=True)


class RowsetCandidateBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    product_id = structure.field(structure.String(), nullable=False)
    product_name = structure.field(structure.String(), nullable=True)


class AdvancedSummaryBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=True)
    grouping_id = structure.field(structure.Long(), nullable=False)
    account_subtotal = structure.field(structure.Boolean(), nullable=False)
    event_count = structure.field(structure.Long(), nullable=False)
    paid_amount = structure.field(structure.Long(), nullable=True)
    any_large = structure.field(structure.Boolean(), nullable=True)
    amount_stddev = structure.field(structure.Double(), nullable=True)
    estimated_events = structure.field(structure.Long(), nullable=False)
    event_ids = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


class AdvancedWindowBatch(structure.Schema):
    account_id = structure.field(structure.String(), nullable=False)
    event_id = structure.field(structure.String(), nullable=False)
    percent_rank = structure.field(structure.Double(), nullable=False)
    cume_dist = structure.field(structure.Double(), nullable=False)
    tile = structure.field(structure.Long(), nullable=False)
    first_event = structure.field(structure.String(), nullable=True)
    last_event = structure.field(structure.String(), nullable=True)
    second_event = structure.field(structure.String(), nullable=True)
    running_amount = structure.field(structure.Long(), nullable=False)
    running_count = structure.field(structure.Long(), nullable=False)


class AdvancedCollectionBatch(structure.Schema):
    event_id = structure.field(structure.String(), nullable=False)
    has_priority = structure.field(structure.Boolean(), nullable=True)
    tags = structure.field(structure.Array(structure.String(), contains_null=True), nullable=True)
    tag_position = structure.field(structure.Long(), nullable=True)
    attribute_keys = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


@transform
class BatchProjectionFeatures(structure.Transform):
    rows = structure.input(RawBatch)
    ranked = structure.output(RankedBatch)

    def rank_events(self, row: RawBatch) -> RankedBatch:
        structure.drop_duplicates(row.account_id, row.event_id)
        structure.dedupe_latest_by(row.sequence, partition_by=row.account_id)
        tags = structure.arr_filter(
            structure.arr_transform(row.tags, lambda tag: structure.lower(trim(tag))), lambda tag: tag.is_not_null()
        )
        attributes = structure.map_filter(
            structure.map_transform_values(row.attributes, lambda key, value: structure.lower(trim(value))),
            lambda key, value: value.is_not_null(),
        )
        return RankedBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            promo_code=row.promo_code,
            event_date=row.event_date,
            sequence=row.sequence,
            amount=row.amount,
            tags=tags,
            attributes=attributes,
            row_number=structure.row_number(partition_by=row.account_id, order_by=row.sequence),
            rank=structure.rank(partition_by=row.account_id, order_by=row.sequence, descending=True),
            dense_rank=structure.dense_rank(partition_by=row.account_id, order_by=row.sequence),
            previous_sequence=structure.lag(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            next_sequence=structure.lead(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            rolling_units=structure.rolling_sum(
                row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_avg_units=structure.rolling_avg(
                row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_min_units=structure.rolling_min(
                row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
            rolling_max_units=structure.rolling_max(
                row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2
            ),
        )


@transform
class BatchRowsetJoinFeatures(structure.Transform):
    rows = structure.input(RawBatch)
    customers = structure.input(Customer)
    products = structure.input(Product)
    matches = structure.output(RowsetMatchBatch)
    backfills = structure.output(RowsetBackfillBatch)
    candidates = structure.output(RowsetCandidateBatch)

    @dsl_step(input=[rows, customers], output=matches)
    def reconcile(self, row: RawBatch, customer: Customer) -> RowsetMatchBatch:
        structure.full_join(on=(row.account_id == customer.id) | (row.event_id == customer.id))
        return RowsetMatchBatch(
            account_id=structure.coalesce(row.account_id, customer.id),
            event_id=row.event_id,
            customer_id=customer.id,
            customer_name=customer.name,
        )

    @dsl_step(input=[rows, customers], output=backfills)
    def keep_customers(self, row: RawBatch, customer: Customer) -> RowsetBackfillBatch:
        structure.right_join(on=customer.id == row.account_id)
        return RowsetBackfillBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            customer_id=customer.id,
            customer_name=customer.name,
        )

    @dsl_step(input=[rows, products], output=candidates)
    def expand_candidates(self, row: RawBatch, product: Product) -> RowsetCandidateBatch:
        structure.cross_join(product, allow_cartesian=True)
        return RowsetCandidateBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            product_id=product.id,
            product_name=product.name,
        )


@transform
class BatchAdvancedAnalyticalFeatures(structure.Transform):
    rows = structure.input(RawBatch)
    summaries = structure.output(AdvancedSummaryBatch)
    cubes = structure.output(AdvancedSummaryBatch)
    windows = structure.output(AdvancedWindowBatch)
    collections = structure.output(AdvancedCollectionBatch)

    @dsl_step(input=rows, output=summaries)
    def summarize(self, row: RawBatch) -> AdvancedSummaryBatch:
        amount = cast(Any, row.amount)
        return (
            structure.rollup(account_id=row.account_id)
            .agg(
                grouping_id=structure.grouping_id(),
                account_subtotal=structure.is_grouped(row.account_id),
                event_count=structure.count(),
                paid_amount=sum(row.amount, where=amount > 0),
                any_large=structure.bool_or(amount > 10),
                amount_stddev=structure.stddev(row.amount),
                estimated_events=structure.approx_count_distinct(row.event_id),
                event_ids=structure.collect_set(row.event_id, element_type=structure.String()),
            )
            .as_schema(AdvancedSummaryBatch)
        )

    @dsl_step(input=rows, output=cubes)
    def summarize_cube(self, row: RawBatch) -> AdvancedSummaryBatch:
        amount = cast(Any, row.amount)
        return (
            structure.cube(account_id=row.account_id)
            .agg(
                grouping_id=structure.grouping_id(),
                account_subtotal=structure.is_grouped(row.account_id),
                event_count=structure.count(),
                paid_amount=sum(row.amount, where=amount > 0),
                any_large=structure.bool_or(amount > 10),
                amount_stddev=structure.stddev(row.amount),
                estimated_events=structure.approx_count_distinct(row.event_id),
                event_ids=structure.collect_set(row.event_id, element_type=structure.String()),
            )
            .as_schema(AdvancedSummaryBatch)
        )

    @dsl_step(input=rows, output=windows)
    def rank_with_reusable_window(self, row: RawBatch) -> AdvancedWindowBatch:
        batch_window = window(
            partition_by=row.account_id,
            order_by=row.sequence,
            frame=structure.rows_between(structure.preceding(2), structure.current_row()),
        )
        return AdvancedWindowBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            percent_rank=structure.percent_rank(over=batch_window),
            cume_dist=structure.cume_dist(over=batch_window),
            tile=structure.ntile(2, over=batch_window),
            first_event=structure.first_value(row.event_id, over=batch_window),
            last_event=structure.last_value(row.event_id, over=batch_window),
            second_event=structure.nth_value(row.event_id, 2, over=batch_window),
            running_amount=window_sum(row.amount, over=batch_window),
            running_count=window_count(over=batch_window),
        )

    @dsl_step(input=rows, output=collections)
    def summarize_collections(self, row: RawBatch) -> AdvancedCollectionBatch:
        clean_attributes = structure.map_filter(
            structure.map_transform_keys(
                structure.map_transform_values(row.attributes, lambda key, value: structure.lower(trim(value))),
                lambda key, value: structure.lower(trim(key)),
            ),
            lambda key, value: value.is_not_null(),
        )
        return AdvancedCollectionBatch(
            event_id=row.event_id,
            has_priority=structure.arr_exists(row.tags, lambda tag: structure.lower(trim(tag)) == "priority"),
            tags=structure.arr_distinct(
                structure.arr_zip_with(row.tags, row.tags, lambda left, right: structure.lower(trim(left)))
            ),
            tag_position=structure.arr_position(row.tags, "priority"),
            attribute_keys=structure.map_keys(clean_attributes),
        )


@transform
class BatchAggregateFeatures(structure.Transform):
    rows = structure.input(RawBatch)
    summary = structure.output(AccountSummary)

    def summarize(self, row: RawBatch) -> AccountSummary:
        return (
            structure.group_by(account_id=row.account_id)
            .agg(
                event_count=structure.count(),
                distinct_events=structure.count_distinct(row.event_id),
                total_amount=sum(row.amount),
                min_amount=structure.min(row.amount),
                max_amount=structure.max(row.amount),
                avg_amount=structure.avg(row.amount),
            )
            .as_schema(AccountSummary)
        )


@transform
class BatchJoinFeatures(structure.Transform):
    rows = structure.input(RawBatch)
    customers = structure.input(Customer)
    products = structure.input(Product)
    blocked_products = structure.input(BlockedProduct)
    promotions = structure.input(Promotion)
    shipments = structure.input(Shipment)
    joined = structure.output(JoinedBatch)

    def add_customer(self, row: RawBatch, customer: Customer) -> CustomerBatch:
        customer = structure.lookup_join(
            customer, on=row.account_id == customer.id, how=structure.Join.LEFT, hint=structure.JoinHint.BROADCAST
        )
        return CustomerBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            promo_code=row.promo_code,
            event_date=row.event_date,
            customer_name=customer.name,
        )

    def add_product(self, row: CustomerBatch, product: Product, blocked_product: BlockedProduct) -> ProductBatch:
        where(structure.exists(on=row.event_id == product.id))
        where(structure.not_exists(on=row.event_id == blocked_product.id))
        product = structure.lookup_join(
            product,
            on=row.event_id == product.id,
            how=structure.Join.LEFT,
            dedupe=structure.JoinDedupe.latest_by(product.ingested_at, ties=structure.TiePolicy.ERROR),
        )
        return ProductBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            promo_code=row.promo_code,
            event_date=row.event_date,
            customer_name=row.customer_name,
            product_name=product.name,
        )

    def add_promotion(self, row: ProductBatch, promotion: Promotion) -> PromotedBatch:
        promotion = temporal_one(
            promotion,
            on=promotion.code == row.promo_code,
            at=row.event_date,
            valid_from=promotion.valid_from,
            valid_to=promotion.valid_to,
            how=structure.Join.LEFT,
        )
        return PromotedBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            customer_name=row.customer_name,
            product_name=row.product_name,
            promotion_name=promotion.name,
        )

    def add_shipments(self, row: PromotedBatch, shipment: Shipment) -> JoinedBatch:
        shipment = structure.inner_join(
            shipment,
            on=row.event_id == shipment.event_id,
            strategy=structure.JoinStrategy.SHUFFLE_HASH,
        )
        return JoinedBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            customer_name=row.customer_name,
            product_name=row.product_name,
            promotion_name=row.promotion_name,
            shipment_line=shipment.line,
        )


def test_completed_batch_feature_rendering_is_identical_for_spark_connect_variant() -> None:
    for transform_class in _completed_batch_transforms():
        ordinary = _lower(transform_class, target_variant="ordinary")
        spark_connect = _lower(transform_class, target_variant="spark-connect")

        assert spark_connect.backend.variant == "spark-connect"
        assert spark_connect.backend.family == "spark_connect_dataframe"
        assert _render(ordinary) == _render(spark_connect)


def test_completed_batch_feature_set_exercises_sprint08_feature_families_under_spark_connect() -> None:
    plans = tuple(
        _lower(transform_class, target_variant="spark-connect") for transform_class in _completed_batch_transforms()
    )

    assert _operation_kinds(plans) >= {"aggregate", "drop_duplicates", "join", "selected_rows"}
    assert _join_methods(plans) >= {
        "exists",
        "lookup_join",
        "rowset_join",
        "not_exists",
        "temporal_one",
    }
    assert _aggregate_kinds(plans) >= {"group_by", "rollup", "cube"}
    assert _operation_capabilities(plans) >= {
        "cross",
        "full",
        "right",
        "rollup",
        "stddev",
        "approx_count_distinct",
        "array_exists",
        "array_zip_with",
        "map_transform_keys",
        "percent_rank",
        "window_sum",
    }


def test_spark_connect_generated_batch_code_avoids_classic_only_internals() -> None:
    rendered = [render_pyspark_runtime_module()]
    rendered.extend(
        _render(_lower(transform_class, target_variant="spark-connect"))
        for transform_class in _completed_batch_transforms()
    )
    text = "\n".join(rendered)

    assert all(token not in text for token in CLASSIC_ONLY_TOKENS)


def test_spark_connect_traceability_shape_matches_ordinary_pyspark_for_completed_batch_features() -> None:
    ordinary = _lower(BatchProjectionFeatures, target_variant="ordinary")
    spark_connect = _lower(BatchProjectionFeatures, target_variant="spark-connect")

    ordinary_trace = Compiler.traceability.build()(
        ordinary,
        source_transform="tests.BatchProjectionFeatures",
        transform_module="tests.BatchProjectionFeaturesGenerated",
    )
    connect_trace = Compiler.traceability.build()(
        spark_connect,
        source_transform="tests.BatchProjectionFeatures",
        transform_module="tests.BatchProjectionFeaturesGenerated",
    )

    assert [record.to_dict() for record in ordinary_trace.provenance] == [
        record.to_dict() for record in connect_trace.provenance
    ]
    assert [dependency.target for dependency in ordinary_trace.static_dataflow] == [
        dependency.target for dependency in connect_trace.static_dataflow
    ]


def _lower(transform_class: type[structure.Transform], *, target_variant: str) -> Any:
    capabilities = Capabilities.resolve()(target_backend="pyspark", target_variant=target_variant)
    return PySpark.plan.lower()(compile_transform(transform_class), capabilities=capabilities)


def _render(plan: Any) -> str:
    return render_pyspark_transform_module(
        plan,
        source_transform=f"tests.{plan.transform}",
        schema_modules={schema: "tests.schemas" for schema in _schemas(plan)},
        runtime_module="tests.runtime",
    )


def _schemas(plan: Any) -> set[type[structure.Schema]]:
    schemas = {input.schema for input in plan.inputs}
    schemas.update(output.output_schema for output in plan.outputs)
    for step in plan.steps:
        schemas.add(step.output_schema)
        schemas.update(result.schema for result in step.results)
    return schemas


def _completed_batch_transforms() -> tuple[type[structure.Transform], ...]:
    return (
        BatchProjectionFeatures,
        BatchAggregateFeatures,
        BatchJoinFeatures,
        BatchRowsetJoinFeatures,
        BatchAdvancedAnalyticalFeatures,
    )


def _operation_kinds(plans: tuple[Any, ...]) -> set[str]:
    kinds = {operation.kind for plan in plans for step in plan.steps for operation in step.operations}
    if any(step.filters for plan in plans for step in plan.steps):
        kinds.add("filter")
    return kinds


def _join_methods(plans: tuple[Any, ...]) -> set[str]:
    return {
        operation.join.method.value
        for plan in plans
        for step in plan.steps
        for operation in step.operations
        if operation.join is not None
    }


def _aggregate_kinds(plans: tuple[Any, ...]) -> set[str]:
    return {step.aggregate.grouping for plan in plans for step in plan.steps if step.aggregate is not None}


def _operation_capabilities(plans: tuple[Any, ...]) -> set[str]:
    names: set[str] = set()
    for plan in plans:
        for step in plan.steps:
            for operation in step.operations:
                if operation.join is not None:
                    names.add(operation.join.method.value)
                    names.add(operation.join.how.value)
            if step.aggregate is not None:
                names.add(step.aggregate.grouping)
                for assignment in step.aggregate.assignments:
                    names.add(assignment.function)
                    if assignment.expression is not None:
                        names.update(_expression_kinds(assignment.expression))
                    for argument in assignment.arguments:
                        names.update(_expression_kinds(argument))
                    if assignment.filter is not None:
                        names.update(_expression_kinds(assignment.filter))
                    if assignment.order_by is not None:
                        names.update(_expression_kinds(assignment.order_by))
            for projection in step.projection:
                names.update(_expression_kinds(projection.expression))
    return names


def _expression_kinds(expression: Any) -> set[str]:
    kinds = {expression.kind}
    capability_name = expression.data.get("capability_name")
    if isinstance(capability_name, str):
        kinds.add(capability_name)
    function = expression.data.get("function")
    if isinstance(function, str):
        kinds.add(function)
    for child in expression.args:
        kinds.update(_expression_kinds(child))
    for value in expression.data.values():
        if hasattr(value, "kind") and hasattr(value, "args"):
            kinds.update(_expression_kinds(value))
        elif isinstance(value, tuple):
            for item in value:
                if hasattr(item, "kind") and hasattr(item, "args"):
                    kinds.update(_expression_kinds(item))
    return kinds
