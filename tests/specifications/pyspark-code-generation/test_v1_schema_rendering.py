import sys

from structure.app.target.pyspark.api import PySpark


def test_v1_schema_rendering_is_spark_free() -> None:
    from testing.model.v1.orders.schemas.common import TenantKey

    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = PySpark.schema.render()(TenantKey)

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert text == (
        "TENANT_KEY_SCHEMA = T.StructType([\n" '    T.StructField("tenant_id", T.StringType(), False),\n' "])"
    )


def test_v1_schema_renderer_maps_primitives_and_nested_structs() -> None:
    from testing.model.v1.orders.schemas.common import AuditStamp, BusinessDate
    from testing.model.v1.orders.schemas.customer import Customer

    assert PySpark.schema.render().field(AuditStamp._structure_fields["ingested_at"]) == (
        '    T.StructField("ingested_at", T.TimestampType(), False),'
    )
    assert PySpark.schema.render().field(BusinessDate._structure_fields["order_date"]) == (
        '    T.StructField("order_date", T.DateType(), True),'
    )
    assert PySpark.schema.render().field(Customer._structure_fields["tenant"]) == (
        '    T.StructField("tenant", TENANT_KEY_SCHEMA, False),'
    )


def test_v1_schema_renderer_maps_collections_and_decimal_fields() -> None:
    from testing.model.v1.orders.schemas.order import OrderNormalized, OrderRaw

    assert PySpark.schema.render().field(OrderRaw._structure_fields["tags"]) == (
        '    T.StructField("tags", T.ArrayType(T.StringType(), containsNull=False), True),'
    )
    assert PySpark.schema.render().field(OrderRaw._structure_fields["attributes"]) == (
        '    T.StructField("attributes", T.MapType(T.StringType(), T.StringType(), valueContainsNull=True), True),'
    )
    assert PySpark.schema.render().field(OrderNormalized._structure_fields["total"]) == (
        '    T.StructField("total", T.DecimalType(12, 2), False),'
    )


def test_v1_schema_renderer_uses_base_schema_composition_for_inheritance() -> None:
    from testing.model.v1.orders.schemas.order import OrderPublished, OrderWithCustomer

    customer = PySpark.schema.render().expression(OrderWithCustomer)
    published = PySpark.schema.render().expression(OrderPublished)

    assert customer.startswith("T.StructType(ORDER_NORMALIZED_SCHEMA.fields + [")
    assert 'T.StructField("customer_name", T.StringType(), True),' in customer
    assert published == "T.StructType(ORDER_PUBLICATION_SCHEMA.fields + PUBLICATION_FLAGS_SCHEMA.fields)"
