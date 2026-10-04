from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import boolean, covar_pop, double, every, group_by, long, mean, some, std, string

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_boolean_aggregates"
PACKAGE = "integration_v7_boolean_aggregates_generated"


class BooleanInput(Schema):
    group = string(nullable=False)
    value = boolean(nullable=True)
    x = long(nullable=True)
    y = long(nullable=True)


class BooleanOutput(Schema):
    group = string(nullable=False)
    all_values_true = boolean(nullable=True)
    some_values_true = boolean(nullable=True)
    average_x = double(nullable=True)
    sample_std_x = double(nullable=True)
    population_covariance = double(nullable=True)


@transform
class BooleanAggregates(Transform):
    rows = input(BooleanInput)
    result = output(BooleanOutput)

    def summarize(self, row: BooleanInput) -> BooleanOutput:
        group_by(row.group)
        return BooleanOutput(
            group=row.group,
            all_values_true=every(row.value),
            some_values_true=some(row.value),
            average_x=mean(row.x),
            sample_std_x=std(row.x),
            population_covariance=covar_pop(row.x, row.y),
        )


def test_every_matches_generated_execution_on_live_backend(spark, tmp_path) -> None:
    files = render_generated_project(
        BooleanAggregates,
        source_transform=f"{SOURCE_MODULE}.BooleanAggregates",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [BooleanInput, BooleanOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_boolean_aggregates.py"
    assert "F.every(" in files[transform_path]
    assert "F.some(" in files[transform_path]
    assert "F.mean(" in files[transform_path]
    assert "F.std(" in files[transform_path]
    assert ".covar_pop(" in files[transform_path]

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_boolean_aggregates")
        source = spark.createDataFrame(
            [
                ("all-true", True, 1, 2),
                ("all-true", True, 1, 2),
                ("mixed", True, 1, 2),
                ("mixed", False, 3, 6),
                ("all-null", None, None, None),
            ],
            generated_schemas.BOOLEAN_INPUT_SCHEMA,
        )
        online = BooleanAggregates(rows=source).run(session(spark, execution_mode="online"))
        generated = BooleanAggregates(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.result, "group")

    assert actual == [
        {
            "group": "all-null",
            "all_values_true": None,
            "some_values_true": None,
            "average_x": None,
            "sample_std_x": None,
            "population_covariance": None,
        },
        {
            "group": "all-true",
            "all_values_true": True,
            "some_values_true": True,
            "average_x": 1,
            "sample_std_x": 0.0,
            "population_covariance": 0.0,
        },
        {
            "group": "mixed",
            "all_values_true": False,
            "some_values_true": True,
            "average_x": 2,
            "sample_std_x": pytest.approx(2**0.5),
            "population_covariance": 2.0,
        },
    ]
