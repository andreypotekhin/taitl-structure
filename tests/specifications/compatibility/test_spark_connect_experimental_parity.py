from typing import Any

from structure import (
    Array,
    Date,
    Double,
    Join,
    JoinDedupe,
    JoinHint,
    JoinStrategy,
    Long,
    Map,
    String,
    Structure,
    TiePolicy,
    Transform,
    arr_filter,
    arr_transform,
    avg,
    count,
    count_distinct,
    dedupe_latest_by,
    dense_rank,
    drop_duplicates,
    exists,
    field,
    group_by,
    input,
    join_many,
    join_one,
    lag,
    lead,
    lower,
    map_filter,
    map_transform_values,
    max,
    min,
    not_exists,
    output,
    rank,
    rolling_avg,
    rolling_max,
    rolling_min,
    rolling_sum,
    row_number,
    sum,
    temporal_one,
    transform,
    trim,
    where,
)
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


class RawBatch(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    promo_code = field(String(), nullable=True)
    event_date = field(Date(), nullable=False)
    sequence = field(Long(), nullable=False)
    amount = field(Long(), nullable=False)
    tags = field(Array(String(), contains_null=True), nullable=True)
    attributes = field(Map(String(), String(), value_contains_null=True), nullable=True)


class RankedBatch(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    promo_code = field(String(), nullable=True)
    event_date = field(Date(), nullable=False)
    sequence = field(Long(), nullable=False)
    amount = field(Long(), nullable=False)
    tags = field(Array(String(), contains_null=True), nullable=True)
    attributes = field(Map(String(), String(), value_contains_null=True), nullable=True)
    row_number = field(Long(), nullable=False)
    rank = field(Long(), nullable=False)
    dense_rank = field(Long(), nullable=False)
    previous_sequence = field(Long(), nullable=True)
    next_sequence = field(Long(), nullable=True)
    rolling_units = field(Long(), nullable=False)
    rolling_avg_units = field(Double(), nullable=False)
    rolling_min_units = field(Long(), nullable=False)
    rolling_max_units = field(Long(), nullable=False)


class AccountSummary(Structure):
    account_id = field(String(), nullable=False)
    event_count = field(Long(), nullable=False)
    distinct_events = field(Long(), nullable=False)
    total_amount = field(Long(), nullable=False)
    min_amount = field(Long(), nullable=False)
    max_amount = field(Long(), nullable=False)
    avg_amount = field(Double(), nullable=False)


class Customer(Structure):
    id = field(String(), nullable=False)
    name = field(String(), nullable=True)


class Product(Structure):
    id = field(String(), nullable=False)
    name = field(String(), nullable=True)
    ingested_at = field(Long(), nullable=False)


class BlockedProduct(Structure):
    id = field(String(), nullable=False)


class Promotion(Structure):
    code = field(String(), nullable=False)
    name = field(String(), nullable=True)
    valid_from = field(Date(), nullable=False)
    valid_to = field(Date(), nullable=True)


class Shipment(Structure):
    event_id = field(String(), nullable=False)
    line = field(Long(), nullable=False)


class CustomerBatch(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    promo_code = field(String(), nullable=True)
    event_date = field(Date(), nullable=False)
    customer_name = field(String(), nullable=True)


class ProductBatch(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    promo_code = field(String(), nullable=True)
    event_date = field(Date(), nullable=False)
    customer_name = field(String(), nullable=True)
    product_name = field(String(), nullable=True)


class PromotedBatch(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    customer_name = field(String(), nullable=True)
    product_name = field(String(), nullable=True)
    promotion_name = field(String(), nullable=True)


class JoinedBatch(Structure):
    account_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    customer_name = field(String(), nullable=True)
    product_name = field(String(), nullable=True)
    promotion_name = field(String(), nullable=True)
    shipment_line = field(Long(), nullable=False)


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
class BatchAggregateFeatures(Transform):
    rows = input(RawBatch)
    summary = output(AccountSummary)

    def summarize(self, row: RawBatch) -> AccountSummary:
        return group_by(account_id=row.account_id).agg(
            event_count=count(),
            distinct_events=count_distinct(row.event_id),
            total_amount=sum(row.amount),
            min_amount=min(row.amount),
            max_amount=max(row.amount),
            avg_amount=avg(row.amount),
        ).as_schema(AccountSummary)


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
        customer = join_one(customer, on=row.account_id == customer.id, how=Join.LEFT, hint=JoinHint.BROADCAST)
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
        product = join_one(
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
        shipment = join_many(
            shipment,
            on=row.event_id == shipment.event_id,
            how=Join.INNER,
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
    for transform_class in (BatchProjectionFeatures, BatchAggregateFeatures, BatchJoinFeatures):
        ordinary = _lower(transform_class, target_variant="ordinary")
        spark_connect = _lower(transform_class, target_variant="spark-connect")

        assert spark_connect.backend.variant == "spark-connect"
        assert spark_connect.backend.family == "spark_connect_dataframe"
        assert _render(ordinary) == _render(spark_connect)


def test_completed_batch_feature_set_exercises_sprint08_feature_families_under_spark_connect() -> None:
    plans = tuple(
        _lower(transform_class, target_variant="spark-connect")
        for transform_class in (BatchProjectionFeatures, BatchAggregateFeatures, BatchJoinFeatures)
    )

    assert _operation_kinds(plans) == {"aggregate", "drop_duplicates", "join", "selected_rows"}
    assert _join_methods(plans) == {"exists", "join_many", "join_one", "not_exists", "temporal_one"}


def test_spark_connect_generated_batch_code_avoids_classic_only_internals() -> None:
    rendered = [render_pyspark_runtime_module()]
    rendered.extend(
        _render(_lower(transform_class, target_variant="spark-connect"))
        for transform_class in (BatchProjectionFeatures, BatchAggregateFeatures, BatchJoinFeatures)
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


def _schemas(plan: Any) -> set[type[Structure]]:
    schemas = {input.schema for input in plan.inputs}
    schemas.update(output.output_schema for output in plan.outputs)
    for step in plan.steps:
        schemas.add(step.output_schema)
        schemas.update(result.schema for result in step.results)
    return schemas


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
