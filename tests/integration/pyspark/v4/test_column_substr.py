from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import boolean, integer, string

pytestmark = pytest.mark.integration

PACKAGE = "integration_v4_column_substr_generated"


class SubstrInput(Schema):
    id = string(nullable=False)
    name = string(nullable=True)
    required_name = string(nullable=False)
    start = integer(nullable=False)
    length = integer(nullable=False)


class SubstrOutput(Schema):
    id = string(nullable=False)
    literal = string(nullable=True)
    dynamic = string(nullable=False)


@transform
class ColumnSubstr(Transform):
    rows = input(SubstrInput)
    output_rows = output(SubstrOutput)

    def publish(self, row: SubstrInput) -> SubstrOutput:
        return SubstrOutput(
            id=row.id,
            literal=row.name.substr(1, 3),
            dynamic=row.required_name.substr(row.start, row.length),
        )


class StringPredicateInput(Schema):
    id = string(nullable=False)
    status = string(nullable=True)
    name = string(nullable=True)
    prefix = string(nullable=True)


class StringPredicateOutput(Schema):
    id = string(nullable=False)
    known = boolean(nullable=True)
    contains_prefix = boolean(nullable=True)


@transform
class ColumnPredicate(Transform):
    rows = input(StringPredicateInput)
    output_rows = output(StringPredicateOutput)

    def publish(self, row: StringPredicateInput) -> StringPredicateOutput:
        return StringPredicateOutput(
            id=row.id,
            known=row.status.isin(["new", "paid"]),
            contains_prefix=row.name.contains(row.prefix),
        )


def test_column_substr_matches_online_and_generated_execution(spark, tmp_path) -> None:
    source_module = ColumnSubstr.__module__
    files = render_generated_project(
        ColumnSubstr,
        source_transform=f"{source_module}.ColumnSubstr",
        generated_package=PACKAGE,
        source_schema_modules={source_module: [SubstrInput, SubstrOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/{source_module.replace('.', '/')}.py"
    assert ".substr(" in files[transform_path]

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_column_substr")
        source = spark.createDataFrame(
            [
                ("row-1", "Ada", "Ada Lovelace", 1, 3),
                ("row-2", None, "Grace Hopper", 1, 5),
            ],
            generated_schemas.SUBSTR_INPUT_SCHEMA,
        )

        online = ColumnSubstr(rows=source).run(session(spark, execution_mode="online"))
        generated = ColumnSubstr(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.output_rows, "id")

    assert actual == [
        {"id": "row-1", "literal": "Ada", "dynamic": "Ada"},
        {"id": "row-2", "literal": None, "dynamic": "Grace"},
    ]


def test_column_predicates_match_online_and_generated_execution(spark, tmp_path) -> None:
    source_module = ColumnPredicate.__module__
    files = render_generated_project(
        ColumnPredicate,
        source_transform=f"{source_module}.ColumnPredicate",
        generated_package="integration_v4_column_predicate_generated",
        source_schema_modules={source_module: [StringPredicateInput, StringPredicateOutput]},
    )
    transform_path = f"integration_v4_column_predicate_generated/pyspark/transforms/{source_module.replace('.', '/')}.py"
    assert '.isin(F.lit(\'new\'), F.lit(\'paid\'))' in files[transform_path]
    assert '.contains(\n                    F.col(\n                        "string_predicate_input.prefix"' in files[transform_path]

    package = "integration_v4_column_predicate_generated"
    with generated_project(tmp_path, package, files):
        generated_schemas = importlib.import_module(f"{package}.pyspark.schemas.test_column_substr")
        source = spark.createDataFrame(
            [
                ("row-1", "new", "Ada Lovelace", "Ada"),
                ("row-2", None, None, "A"),
                ("row-3", "closed", "Grace Hopper", "Grace"),
            ],
            generated_schemas.STRING_PREDICATE_INPUT_SCHEMA,
        )

        online = ColumnPredicate(rows=source).run(session(spark, execution_mode="online"))
        generated = ColumnPredicate(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.output_rows, "id")

    assert actual == [
        {"id": "row-1", "known": True, "contains_prefix": True},
        {"id": "row-2", "known": None, "contains_prefix": None},
        {"id": "row-3", "known": False, "contains_prefix": True},
    ]
