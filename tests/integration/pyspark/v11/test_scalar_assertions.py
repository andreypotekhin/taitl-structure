from __future__ import annotations

import importlib
from typing import cast

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import assert_true, boolean, raise_error, string, where

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v11.test_scalar_assertions"
PACKAGE = "integration_v11_scalar_assertions_generated"


class AssertionInput(Schema):
    id = string(nullable=False)
    accepted = boolean(nullable=True)


class AssertionOutput(Schema):
    id = string(nullable=False)


@transform
class RequireAccepted(Transform):
    rows = input(AssertionInput)
    accepted_rows = output(AssertionOutput)

    def publish(self, row: AssertionInput) -> AssertionOutput:
        return cast(
            AssertionOutput,
            where(assert_true(row.accepted, message="accepted is required")).project(row, AssertionOutput),
        )


@transform
class Stop(Transform):
    rows = input(AssertionInput)
    stopped_rows = output(AssertionOutput)

    def publish(self, row: AssertionInput) -> AssertionOutput:
        return cast(AssertionOutput, where(raise_error("deliberately stopped")).project(row, AssertionOutput))


def test_scalar_assertions_match_online_and_generated_execution(spark, tmp_path) -> None:
    files = render_generated_project(
        RequireAccepted,
        source_transform=f"{SOURCE_MODULE}.RequireAccepted",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [AssertionInput, AssertionOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v11/test_scalar_assertions.py"
    assert (
        'F.assert_true(F.col("assertion_input.accepted"), \'accepted is required\').isNull()' in files[transform_path]
    )

    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_scalar_assertions")
        source = spark.createDataFrame([("accepted", True)], schemas.ASSERTION_INPUT_SCHEMA)
        online = RequireAccepted(rows=source).run(session(spark, execution_mode="online"))
        generated = RequireAccepted(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )

        assert_online_generated_parity(lambda: online, lambda: generated)
        assert rows(generated.accepted_rows) == [{"id": "accepted"}]


@pytest.mark.parametrize("accepted", [False, None])
def test_assert_true_raises_for_false_and_null_in_both_execution_modes(spark, tmp_path, accepted) -> None:
    files = render_generated_project(
        RequireAccepted,
        source_transform=f"{SOURCE_MODULE}.RequireAccepted",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [AssertionInput, AssertionOutput]},
    )

    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_scalar_assertions")
        source = spark.createDataFrame([("rejected", accepted)], schemas.ASSERTION_INPUT_SCHEMA)
        online = RequireAccepted(rows=source).run(session(spark, execution_mode="online"))
        generated = RequireAccepted(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )

        for result in (online.accepted_rows, generated.accepted_rows):
            with pytest.raises(Exception, match="accepted is required"):
                result.collect()


def test_raise_error_raises_in_both_execution_modes(spark, tmp_path) -> None:
    files = render_generated_project(
        Stop,
        source_transform=f"{SOURCE_MODULE}.Stop",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [AssertionInput, AssertionOutput]},
    )

    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_scalar_assertions")
        source = spark.createDataFrame([("stopped", True)], schemas.ASSERTION_INPUT_SCHEMA)
        online = Stop(rows=source).run(session(spark, execution_mode="online"))
        generated = Stop(rows=source).run(session(spark, execution_mode="generated", generated_package=PACKAGE))

        for result in (online.stopped_rows, generated.stopped_rows):
            with pytest.raises(Exception, match="deliberately stopped"):
                result.collect()
