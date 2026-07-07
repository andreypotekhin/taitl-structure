from __future__ import annotations

import pytest
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows
from integration.pyspark.v2.support import rowset_joins

pytestmark = pytest.mark.integration


def test_online_and_generated_execution_match_rowset_joins_on_live_backend(spark, tmp_path) -> None:
    RowsetJoinExamples = rowset_joins.transform()
    generated_package = "integration_v2_rowset_generated"
    files = render_generated_project(
        RowsetJoinExamples,
        source_transform="testing.model.v2.orders.transforms.rowset_join.RowsetJoinExamples",
        generated_package=generated_package,
        source_schema_modules=rowset_joins.source_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = rowset_joins.generated_schemas(generated_package)
        inputs = rowset_joins.input_frames(spark, schemas)
        generated = RowsetJoinExamples(**inputs).run(
            session(spark, execution_mode="generated", generated_package=generated_package)
        )
        online = RowsetJoinExamples(**inputs).run(session(spark, execution_mode="online"))

        generated_rows = rows(generated.candidates, "customer_id", "product_id")
        assert rows(online.candidates, "customer_id", "product_id") == generated_rows
        assert generated_rows == [
            {
                "tenant_id": "t1",
                "order_id": "o-1",
                "customer_id": "c-1",
                "customer_name": "Ada Lovelace",
                "product_id": "p-1",
                "product_name": "Analytical Engine",
            },
            {
                "tenant_id": "t1",
                "order_id": "o-1",
                "customer_id": "c-1",
                "customer_name": "Ada Lovelace",
                "product_id": "p-2",
                "product_name": "Compiler",
            },
            {
                "tenant_id": "t1",
                "order_id": None,
                "customer_id": "c-2",
                "customer_name": "Grace Hopper",
                "product_id": "p-1",
                "product_name": "Analytical Engine",
            },
            {
                "tenant_id": "t1",
                "order_id": None,
                "customer_id": "c-2",
                "customer_name": "Grace Hopper",
                "product_id": "p-2",
                "product_name": "Compiler",
            },
        ]

    assert_generated_connect_safe(files)
