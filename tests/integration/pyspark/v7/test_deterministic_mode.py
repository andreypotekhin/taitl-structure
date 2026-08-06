from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import group_by, mode, string

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_deterministic_mode"
PACKAGE = "integration_v7_deterministic_mode_generated"


class CustomerEvent(Schema):
    customer_id = string(nullable=False)
    category = string(nullable=True)


class CustomerPreference(Schema):
    customer_id = string(nullable=False)
    preferred_category = string(nullable=True)


@transform
class ChooseCustomerPreference(Transform):
    rows = input(CustomerEvent)
    preferences = output(CustomerPreference)

    def summarize(self, row: CustomerEvent) -> CustomerPreference:
        group_by(row.customer_id)
        return CustomerPreference(
            customer_id=row.customer_id,
            preferred_category=mode(row.category, deterministic=True),
        )


def test_v7_deterministic_mode_matches_generated_execution_on_live_backend(spark, tmp_path) -> None:
    files = render_generated_project(
        ChooseCustomerPreference,
        source_transform=f"{SOURCE_MODULE}.ChooseCustomerPreference",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [CustomerEvent, CustomerPreference]},
    )
    transform_source = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_deterministic_mode.py"]
    assert "F.collect_list(" in transform_source
    assert ".array_min(" in transform_source
    assert "_structure_count" in transform_source

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_deterministic_mode")
        source = spark.createDataFrame(
            [
                ("customer-1", "books"),
                ("customer-1", "toys"),
                ("customer-1", "books"),
                ("customer-2", "toys"),
                ("customer-2", "books"),
                ("customer-2", None),
                ("customer-3", None),
            ],
            generated_schemas.CUSTOMER_EVENT_SCHEMA,
        )

        online = ChooseCustomerPreference(rows=source).run(session(spark, execution_mode="online"))
        generated = ChooseCustomerPreference(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)

        actual = rows(generated.preferences, "customer_id")

    assert actual == [
        {"customer_id": "customer-1", "preferred_category": "books"},
        {"customer_id": "customer-2", "preferred_category": "books"},
        {"customer_id": "customer-3", "preferred_category": None},
    ]
