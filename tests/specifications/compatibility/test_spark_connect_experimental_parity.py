from typing import Any, cast

from structure import *
from structure.app.compiler.api import Compiler
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


class RawBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    promo_code = field.string(nullable=True)
    event_date = field.date(nullable=False)
    sequence = field.long(nullable=False)
    amount = field.long(nullable=False)
    tags = field.array(field.string(), contains_null=True, nullable=True)
    attributes = field.map(field.string(), field.string(), value_contains_null=True, nullable=True)


class RankedBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    promo_code = field.string(nullable=True)
    event_date = field.date(nullable=False)
    sequence = field.long(nullable=False)
    amount = field.long(nullable=False)
    tags = field.array(field.string(), contains_null=True, nullable=True)
    attributes = field.map(field.string(), field.string(), value_contains_null=True, nullable=True)
    row_number = field.long(nullable=False)
    rank = field.long(nullable=False)
    dense_rank = field.long(nullable=False)
    previous_sequence = field.long(nullable=True)
    next_sequence = field.long(nullable=True)
    rolling_units = field.long(nullable=False)
    rolling_avg_units = field.double(nullable=False)
    rolling_min_units = field.long(nullable=False)
    rolling_max_units = field.long(nullable=False)


class AccountSummary(Schema):
    account_id = field.string(nullable=False)
    event_count = field.long(nullable=False)
    distinct_events = field.long(nullable=False)
    total_amount = field.long(nullable=False)
    min_amount = field.long(nullable=False)
    max_amount = field.long(nullable=False)
    avg_amount = field.double(nullable=False)


class Customer(Schema):
    id = field.string(nullable=False)
    name = field.string(nullable=True)


class Product(Schema):
    id = field.string(nullable=False)
    name = field.string(nullable=True)
    ingested_at = field.long(nullable=False)


class BlockedProduct(Schema):
    id = field.string(nullable=False)


class Promotion(Schema):
    code = field.string(nullable=False)
    name = field.string(nullable=True)
    valid_from = field.date(nullable=False)
    valid_to = field.date(nullable=True)


class Shipment(Schema):
    event_id = field.string(nullable=False)
    line = field.long(nullable=False)


class CustomerBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    promo_code = field.string(nullable=True)
    event_date = field.date(nullable=False)
    customer_name = field.string(nullable=True)


class ProductBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    promo_code = field.string(nullable=True)
    event_date = field.date(nullable=False)
    customer_name = field.string(nullable=True)
    product_name = field.string(nullable=True)


class PromotedBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    customer_name = field.string(nullable=True)
    product_name = field.string(nullable=True)
    promotion_name = field.string(nullable=True)


class JoinedBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    customer_name = field.string(nullable=True)
    product_name = field.string(nullable=True)
    promotion_name = field.string(nullable=True)
    shipment_line = field.long(nullable=False)


class RowsetMatchBatch(Schema):
    account_id = field.string(nullable=True)
    event_id = field.string(nullable=True)
    customer_id = field.string(nullable=True)
    customer_name = field.string(nullable=True)


class RowsetBackfillBatch(Schema):
    account_id = field.string(nullable=True)
    event_id = field.string(nullable=True)
    customer_id = field.string(nullable=False)
    customer_name = field.string(nullable=True)


class RowsetCandidateBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    product_id = field.string(nullable=False)
    product_name = field.string(nullable=True)


class AdvancedSummaryBatch(Schema):
    account_id = field.string(nullable=True)
    grouping_id = field.long(nullable=False)
    account_subtotal = field.boolean(nullable=False)
    event_count = field.long(nullable=False)
    paid_amount = field.long(nullable=True)
    any_large = field.boolean(nullable=True)
    amount_stddev = field.double(nullable=True)
    estimated_events = field.long(nullable=False)
    event_ids = field.array(field.string(), contains_null=False, nullable=True)


class AdvancedWindowBatch(Schema):
    account_id = field.string(nullable=False)
    event_id = field.string(nullable=False)
    percent_rank = field.double(nullable=False)
    cume_dist = field.double(nullable=False)
    tile = field.long(nullable=False)
    first_event = field.string(nullable=True)
    last_event = field.string(nullable=True)
    second_event = field.string(nullable=True)
    running_amount = field.long(nullable=False)
    running_count = field.long(nullable=False)


class AdvancedCollectionBatch(Schema):
    event_id = field.string(nullable=False)
    has_priority = field.boolean(nullable=True)
    tags = field.array(field.string(), contains_null=True, nullable=True)
    tag_position = field.long(nullable=True)
    attribute_keys = field.array(field.string(), contains_null=False, nullable=True)


@transform
class BatchProjectionFeatures(Transform):
    rows = input(RawBatch)
    ranked = output(RankedBatch)

    def rank_events(self, row: RawBatch) -> RankedBatch:
        drop_duplicates(row.account_id, row.event_id)
        dedupe_latest_by(row.sequence, partition_by=row.account_id)
        tags = arr_filter(arr_transform(row.tags, lambda tag: lower(trim(tag))), lambda tag: tag.is_not_null())
        attributes = map_filter(
            map_transform_values(row.attributes, lambda key, value: lower(trim(value))),
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
            row_number=row_number(partition_by=row.account_id, order_by=row.sequence),
            rank=rank(partition_by=row.account_id, order_by=row.sequence, descending=True),
            dense_rank=dense_rank(partition_by=row.account_id, order_by=row.sequence),
            previous_sequence=lag(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            next_sequence=lead(row.sequence, partition_by=row.account_id, order_by=row.sequence),
            rolling_units=rolling_sum(row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2),
            rolling_avg_units=rolling_avg(row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2),
            rolling_min_units=rolling_min(row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2),
            rolling_max_units=rolling_max(row.amount, partition_by=row.account_id, order_by=row.sequence, preceding=2),
        )


@transform
class BatchRowsetJoinFeatures(Transform):
    rows = input(RawBatch)
    customers = input(Customer)
    products = input(Product)
    matches = output(RowsetMatchBatch)
    backfills = output(RowsetBackfillBatch)
    candidates = output(RowsetCandidateBatch)

    @step(input=[rows, customers], output=matches)
    def reconcile(self, row: RawBatch, customer: Customer) -> RowsetMatchBatch:
        full_join(on=(row.account_id == customer.id) | (row.event_id == customer.id))
        return RowsetMatchBatch(
            account_id=coalesce(row.account_id, customer.id),
            event_id=row.event_id,
            customer_id=customer.id,
            customer_name=customer.name,
        )

    @step(input=[rows, customers], output=backfills)
    def keep_customers(self, row: RawBatch, customer: Customer) -> RowsetBackfillBatch:
        right_join(on=customer.id == row.account_id)
        return RowsetBackfillBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            customer_id=customer.id,
            customer_name=customer.name,
        )

    @step(input=[rows, products], output=candidates)
    def expand_candidates(self, row: RawBatch, product: Product) -> RowsetCandidateBatch:
        cross_join(product, allow_cartesian=True)
        return RowsetCandidateBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            product_id=product.id,
            product_name=product.name,
        )


@transform
class BatchAdvancedAnalyticalFeatures(Transform):
    rows = input(RawBatch)
    summaries = output(AdvancedSummaryBatch)
    cubes = output(AdvancedSummaryBatch)
    windows = output(AdvancedWindowBatch)
    collections = output(AdvancedCollectionBatch)

    @step(input=rows, output=summaries)
    def summarize(self, row: RawBatch) -> AdvancedSummaryBatch:
        amount = cast(Any, row.amount)
        rollup(account_id=row.account_id)
        return AdvancedSummaryBatch(
            account_id=row.account_id,
            grouping_id=grouping_id(),
            account_subtotal=is_grouped(row.account_id),
            event_count=count(),
            paid_amount=sum(row.amount, where=amount > 0),
            any_large=bool_or(amount > 10),
            amount_stddev=stddev(row.amount),
            estimated_events=approx_count_distinct(row.event_id),
            event_ids=collect_set(row.event_id, element_type=types.string()),
        )

    @step(input=rows, output=cubes)
    def summarize_cube(self, row: RawBatch) -> AdvancedSummaryBatch:
        amount = cast(Any, row.amount)
        cube(account_id=row.account_id)
        return AdvancedSummaryBatch(
            account_id=row.account_id,
            grouping_id=grouping_id(),
            account_subtotal=is_grouped(row.account_id),
            event_count=count(),
            paid_amount=sum(row.amount, where=amount > 0),
            any_large=bool_or(amount > 10),
            amount_stddev=stddev(row.amount),
            estimated_events=approx_count_distinct(row.event_id),
            event_ids=collect_set(row.event_id, element_type=types.string()),
        )

    @step(input=rows, output=windows)
    def rank_with_reusable_window(self, row: RawBatch) -> AdvancedWindowBatch:
        batch_window = window(
            partition_by=row.account_id,
            order_by=row.sequence,
            frame=rows_between(preceding(2), current_row()),
        )
        return AdvancedWindowBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            percent_rank=percent_rank(over=batch_window),
            cume_dist=cume_dist(over=batch_window),
            tile=ntile(2, over=batch_window),
            first_event=first_value(row.event_id, over=batch_window),
            last_event=last_value(row.event_id, over=batch_window),
            second_event=nth_value(row.event_id, 2, over=batch_window),
            running_amount=window_sum(row.amount, over=batch_window),
            running_count=window_count(over=batch_window),
        )

    @step(input=rows, output=collections)
    def summarize_collections(self, row: RawBatch) -> AdvancedCollectionBatch:
        clean_attributes = map_filter(
            map_transform_keys(
                map_transform_values(row.attributes, lambda key, value: lower(trim(value))),
                lambda key, value: lower(trim(key)),
            ),
            lambda key, value: value.is_not_null(),
        )
        return AdvancedCollectionBatch(
            event_id=row.event_id,
            has_priority=arr_exists(row.tags, lambda tag: lower(trim(tag)) == "priority"),
            tags=arr_distinct(arr_zip_with(row.tags, row.tags, lambda left, right: lower(trim(left)))),
            tag_position=arr_position(row.tags, "priority"),
            attribute_keys=map_keys(clean_attributes),
        )


@transform
class BatchAggregateFeatures(Transform):
    rows = input(RawBatch)
    summary = output(AccountSummary)

    def summarize(self, row: RawBatch) -> AccountSummary:
        group_by(account_id=row.account_id)
        return AccountSummary(
            account_id=row.account_id,
            event_count=count(),
            distinct_events=count_distinct(row.event_id),
            total_amount=sum(row.amount),
            min_amount=min(row.amount),
            max_amount=max(row.amount),
            avg_amount=avg(row.amount),
        )


@transform
class BatchJoinFeatures(Transform):
    rows = input(RawBatch)
    customers = input(Customer)
    products = input(Product)
    blocked_products = input(BlockedProduct)
    promotions = input(Promotion)
    shipments = input(Shipment)
    joined = output(JoinedBatch)

    def add_customer(self, row: RawBatch, customer: Customer) -> CustomerBatch:
        customer = lookup_join(customer, on=row.account_id == customer.id, how=Join.LEFT, hint=JoinHint.BROADCAST)
        return CustomerBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            promo_code=row.promo_code,
            event_date=row.event_date,
            customer_name=customer.name,
        )

    def add_product(self, row: CustomerBatch, product: Product, blocked_product: BlockedProduct) -> ProductBatch:
        where(exists(on=row.event_id == product.id))
        where(not_exists(on=row.event_id == blocked_product.id))
        product = lookup_join(
            product,
            on=row.event_id == product.id,
            how=Join.LEFT,
            dedupe=JoinDedupe.latest_by(product.ingested_at, ties=TiePolicy.ERROR),
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
            how=Join.LEFT,
        )
        return PromotedBatch(
            account_id=row.account_id,
            event_id=row.event_id,
            customer_name=row.customer_name,
            product_name=row.product_name,
            promotion_name=promotion.name,
        )

    def add_shipments(self, row: PromotedBatch, shipment: Shipment) -> JoinedBatch:
        shipment = inner_join(
            shipment,
            on=row.event_id == shipment.event_id,
            strategy=JoinStrategy.SHUFFLE_HASH,
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


def _lower(transform_class: type[Transform], *, target_variant: str) -> Any:
    capabilities = Capabilities.resolve()(target_backend="pyspark", target_variant=target_variant)
    return PySpark.plan.lower()(compile_transform(transform_class), capabilities=capabilities)


def _render(plan: Any) -> str:
    return render_pyspark_transform_module(
        plan,
        source_transform=f"tests.{plan.transform}",
        schema_modules={schema: "tests.schemas" for schema in _schemas(plan)},
        runtime_module="tests.runtime",
    )


def _schemas(plan: Any) -> set[type[Schema]]:
    schemas = {input.schema for input in plan.inputs}
    schemas.update(output.output_schema for output in plan.outputs)
    for step in plan.steps:
        schemas.add(step.output_schema)
        schemas.update(result.schema for result in step.results)
    return schemas


def _completed_batch_transforms() -> tuple[type[Transform], ...]:
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
