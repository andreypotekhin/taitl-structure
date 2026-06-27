import ast


def test_generated_transform_is_reviewable_python_with_stable_entrypoint(orders_transform_text) -> None:
    """I can inspect generated PySpark code."""

    ast.parse(orders_transform_text)
    assert "class EnrichOrdersGenerated:" in orders_transform_text
    assert "        orders: DataFrame," in orders_transform_text
    assert "        promotions: DataFrame," in orders_transform_text
    assert (
        '        return TransformResult({"published": published}, single=True, '
        'schema={"published": ORDER_PUBLISHED_SCHEMA})' in orders_transform_text
    )


def test_generated_transform_uses_dataframe_and_column_operations(orders_transform_text) -> None:
    """I can expect generated code to use PySpark DataFrame and Column operations."""

    assert "        orders = orders.where(" in orders_transform_text
    assert "        orders = orders.select(" in orders_transform_text
    assert "        orders = orders.join(" in orders_transform_text
    assert "F.lower(F.trim(" in orders_transform_text
    assert "F.coalesce(" in orders_transform_text
    assert "project_schema(published, ORDER_PUBLISHED_SCHEMA)" in orders_transform_text

    forbidden = ("udf(", "pandas_udf(", ".rdd", ".collect(", ".toPandas(")
    assert not any(token in orders_transform_text for token in forbidden)
