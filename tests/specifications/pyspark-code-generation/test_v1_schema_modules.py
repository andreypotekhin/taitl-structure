import sys

from structure.app.target.pyspark.api import render_pyspark_schema_module


def test_v1_common_schema_module_renders_without_pyspark() -> None:
    from testing.model.v1.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey

    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = render_pyspark_schema_module([TenantKey, AuditStamp, Address, BusinessDate])

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert text.startswith("from pyspark.sql import types as T\n\n\nTENANT_KEY_SCHEMA = T.StructType([")
    assert "AUDIT_STAMP_SCHEMA = T.StructType([" in text
    assert '    T.StructField("ingested_at", T.TimestampType(), False),' in text
    assert "BUSINESS_DATE_SCHEMA = T.StructType([" in text
    assert '    T.StructField("order_date", T.DateType(), True),' in text


def test_v1_order_schema_module_renders_nested_schema_imports() -> None:
    from testing.model.v1.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.v1.orders.schemas.order import (
        OrderNormalized,
        OrderPublication,
        OrderPublished,
        OrderRaw,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
        PublicationFlags,
    )

    common_module = "testing.model.v1.structure_generated.orders.pyspark.schemas.common"
    text = render_pyspark_schema_module(
        [
            OrderRaw,
            OrderNormalized,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            OrderPublication,
            PublicationFlags,
            OrderPublished,
        ],
        dependency_modules={
            Address: common_module,
            AuditStamp: common_module,
            BusinessDate: common_module,
            TenantKey: common_module,
        },
    )

    assert (
        "from testing.model.v1.structure_generated.orders.pyspark.schemas.common import "
        "ADDRESS_SCHEMA, AUDIT_STAMP_SCHEMA, BUSINESS_DATE_SCHEMA, TENANT_KEY_SCHEMA"
    ) in text
    assert '    T.StructField("tags", T.ArrayType(T.StringType(), containsNull=False), True),' in text
    assert (
        '    T.StructField("attributes", T.MapType(T.StringType(), T.StringType(), valueContainsNull=True), True),'
    ) in text
    assert "ORDER_WITH_CUSTOMER_SCHEMA = T.StructType(ORDER_NORMALIZED_SCHEMA.fields + [" in text
    assert (
        "ORDER_PUBLISHED_SCHEMA = T.StructType(ORDER_PUBLICATION_SCHEMA.fields + PUBLICATION_FLAGS_SCHEMA.fields)"
        in text
    )
