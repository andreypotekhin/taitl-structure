from typing import Any, cast

import pytest

from structure import (
    Boolean,
    Decimal,
    Integer,
    Join,
    Long,
    String,
    Structure,
    StructureCompileError,
    Transform,
    coalesce,
    field,
    input,
    join_one,
    output,
    to_decimal,
    transform,
    where,
)
from structure.app.dsl.api import compile_transform


class Raw(Structure):
    id = field(String(), nullable=False)


class Clean(Structure):
    id = field(String(), nullable=False)


class Published(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=False)


class NullableRaw(Structure):
    id = field(String(), nullable=False, primary_key=True)
    optional_id = field(String(), nullable=True)
    amount = field(String(), nullable=True)
    count = field(Integer(), nullable=False)


class Lookup(Structure):
    id = field(String(), nullable=False, primary_key=True)
    group = field(String(), nullable=False)
    label = field(String(), nullable=False)


class OptionalClean(Structure):
    optional_id = field(String(), nullable=False)


class MoneyClean(Structure):
    amount = field(Decimal(12, 2), nullable=False)
    count = field(Long(), nullable=False)


class FlagClean(Structure):
    is_paid = field(Boolean(), nullable=False)


class LabelClean(Structure):
    label = field(String(), nullable=False)


def test_v1_unsupported_python_boolean_expression_reports_dsl_diagnostic() -> None:
    @transform
    class BadBoolean(Transform):
        rows = input(Raw)
        clean = output(Clean)

        def normalize(self, row: Raw) -> Clean:
            if row.id:
                return Clean(id=row.id)
            return Clean(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadBoolean)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.docs == "docs/Diagnostics.md#dsl-e0401"
    assert diagnostic.source.endswith("BadBoolean.normalize")
    assert "unsupported symbolic code" in diagnostic.problem_text()
    assert "Structure expression helpers" in diagnostic.use_text()


def test_v1_schema_flow_mismatch_reports_transform_structure_diagnostic() -> None:
    @transform
    class BadFlow(Transform):
        rows = input(Raw)
        published = output(Published)

        def normalize(self, row: Raw) -> Clean:
            return Clean(id=row.id)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id, status="ready")

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadFlow)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert diagnostic.context == {"expected": "Raw", "actual": "Clean"}
    assert diagnostic.source.endswith("BadFlow.publish")
    assert "previous subtransform returns Clean" in diagnostic.problem_text()


def test_v1_missing_output_field_reports_transform_structure_diagnostic() -> None:
    @transform
    class MissingOutput(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(MissingOutput)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert diagnostic.context == {"field": "status", "schema": "Published"}
    assert diagnostic.source.endswith("MissingOutput.publish")
    assert "Published.status is not assigned" in diagnostic.problem_text()


def test_v1_nullable_assignment_to_non_nullable_field_reports_schema_diagnostic() -> None:
    @transform
    class BadNullability(Transform):
        rows = input(NullableRaw)
        clean = output(OptionalClean)

        def normalize(self, row: NullableRaw) -> OptionalClean:
            return OptionalClean(optional_id=row.optional_id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadNullability)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert diagnostic.docs == "docs/Diagnostics.md#schema-e0301"
    assert diagnostic.context == {"field": "optional_id", "schema": "OptionalClean"}
    assert "may produce null" in diagnostic.problem_text()
    assert "where(value.is_not_null())" in diagnostic.use_text()


def test_v1_where_is_not_null_guard_allows_non_nullable_assignment() -> None:
    @transform
    class GuardedNullability(Transform):
        rows = input(NullableRaw)
        clean = output(OptionalClean)

        def normalize(self, row: NullableRaw) -> OptionalClean:
            where(cast(Any, row.optional_id).is_not_null())
            return OptionalClean(optional_id=row.optional_id)

    plan = compile_transform(GuardedNullability)

    assert plan.steps[0].projection[0].field.name == "optional_id"


def test_v1_string_to_decimal_assignment_requires_explicit_conversion() -> None:
    @transform
    class BadConversion(Transform):
        rows = input(NullableRaw)
        clean = output(MoneyClean)

        def normalize(self, row: NullableRaw) -> MoneyClean:
            return MoneyClean(amount=row.amount, count=row.count)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadConversion)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert diagnostic.context == {"field": "amount", "schema": "MoneyClean"}


def test_v1_non_nullable_string_to_decimal_assignment_reports_conversion_diagnostic() -> None:
    class NonNullAmount(Structure):
        amount = field(String(), nullable=False)
        count = field(Integer(), nullable=False)

    @transform
    class BadConversion(Transform):
        rows = input(NonNullAmount)
        clean = output(MoneyClean)

        def normalize(self, row: NonNullAmount) -> MoneyClean:
            return MoneyClean(amount=row.amount, count=row.count)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadConversion)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0302"
    assert diagnostic.docs == "docs/Diagnostics.md#schema-e0302"
    assert diagnostic.context == {"field": "amount", "expected": "Decimal(12, 2)", "actual": "string()"}
    assert "to_decimal" in diagnostic.use_text()


def test_v1_accepted_coercions_compile_without_schema_diagnostics() -> None:
    @transform
    class GoodCoercions(Transform):
        rows = input(NullableRaw)
        clean = output(MoneyClean)

        def normalize(self, row: NullableRaw) -> MoneyClean:
            amount = coalesce(to_decimal(row.amount, precision=12, scale=2), 0)
            return MoneyClean(amount=amount, count=row.count)

    plan = compile_transform(GoodCoercions)

    projection = {assignment.field.name: assignment.expression for assignment in plan.steps[0].projection}
    amount_type = cast(Any, projection["amount"].type)
    count_type = cast(Any, projection["count"].type)
    assert amount_type.name == "decimal"
    assert amount_type.precision == 12
    assert amount_type.scale == 2
    assert count_type.name == "integer"


def test_v1_incompatible_assignment_reports_schema_diagnostic() -> None:
    @transform
    class BadBooleanAssignment(Transform):
        rows = input(NullableRaw)
        clean = output(FlagClean)

        def normalize(self, row: NullableRaw) -> FlagClean:
            return FlagClean(is_paid=row.count)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadBooleanAssignment)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0303"
    assert diagnostic.docs == "docs/Diagnostics.md#schema-e0303"
    assert diagnostic.context == {"field": "is_paid", "expected": "boolean()", "actual": "integer()"}
    assert "value > 0" in diagnostic.use_text()


def test_v1_left_joined_non_nullable_field_is_nullable_until_guarded() -> None:
    @transform
    class BadLeftJoinNullability(Transform):
        rows = input(Raw)
        lookup = input(Lookup)
        clean = output(LabelClean)

        def normalize(self, row: Raw) -> LabelClean:
            item = join_one(self.lookup, on=self.lookup.id == row.id, how=Join.LEFT)
            return LabelClean(label=item.label)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadLeftJoinNullability)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert diagnostic.context == {"field": "label", "schema": "LabelClean"}


def test_v1_join_on_primary_key_compiles_without_uniqueness_warning() -> None:
    @transform
    class UniqueJoin(Transform):
        rows = input(Raw)
        lookup = input(Lookup)
        clean = output(Clean)

        def normalize(self, row: Raw) -> Clean:
            join_one(self.lookup, on=self.lookup.id == row.id, how=Join.LEFT)
            return Clean(id=row.id)

    plan = compile_transform(UniqueJoin)

    assert [diagnostic.code for diagnostic in plan.diagnostics] == []


def test_v1_unproven_join_one_key_emits_uniqueness_warning() -> None:
    @transform
    class UnprovenJoin(Transform):
        rows = input(Raw)
        lookup = input(Lookup)
        clean = output(Clean)

        def normalize(self, row: Raw) -> Clean:
            join_one(self.lookup, on=self.lookup.group == row.id, how=Join.LEFT)
            return Clean(id=row.id)

    plan = compile_transform(UnprovenJoin)

    assert [diagnostic.code for diagnostic in plan.diagnostics] == ["JOIN-W0601"]
    assert plan.diagnostics[0].docs == "docs/Diagnostics.md#join-w0601"
    assert plan.diagnostics[0].context == {"input": "lookup", "occurrence": "1"}


def test_v1_or_join_condition_reports_join_diagnostic() -> None:
    @transform
    class BadJoinCondition(Transform):
        rows = input(Raw)
        lookup = input(Lookup)
        clean = output(Clean)

        def normalize(self, row: Raw) -> Clean:
            join_one(self.lookup, on=(self.lookup.id == row.id) | (self.lookup.group == row.id), how=Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadJoinCondition)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "JOIN-E0601"
    assert diagnostic.docs == "docs/Diagnostics.md#join-e0601"
    assert diagnostic.context == {"input": "lookup", "occurrence": "1"}
    assert "equality key pairs combined with AND" in diagnostic.problem_text()


def test_v1_same_side_join_condition_reports_join_diagnostic() -> None:
    @transform
    class SameSideJoin(Transform):
        rows = input(Raw)
        lookup = input(Lookup)
        clean = output(Clean)

        def normalize(self, row: Raw) -> Clean:
            join_one(self.lookup, on=self.lookup.id == self.lookup.group, how=Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(SameSideJoin)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "joined input with the current row" in raised.value.diagnostic.problem_text()


def test_v1_incompatible_join_key_types_report_join_diagnostic() -> None:
    @transform
    class IncompatibleJoin(Transform):
        rows = input(NullableRaw)
        lookup = input(Lookup)
        clean = output(MoneyClean)

        def normalize(self, row: NullableRaw) -> MoneyClean:
            join_one(self.lookup, on=self.lookup.id == row.count, how=Join.LEFT)
            return MoneyClean(amount=coalesce(to_decimal(row.amount, precision=12, scale=2), 0), count=row.count)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(IncompatibleJoin)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "JOIN-E0601"
    assert "Join key types are incompatible" in diagnostic.problem_text()


def test_v1_member_join_one_reports_migration_diagnostic() -> None:
    @transform
    class MemberJoin(Transform):
        rows = input(Raw)
        lookup = input(Lookup)
        clean = output(Clean)

        def normalize(self, row: Raw) -> Clean:
            self.lookup.join_one(on=self.lookup.id == row.id, how=Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(MemberJoin)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert "self.customers.join_one(...) is no longer supported" in diagnostic.problem_text()
    assert "join_one(self.customers, on=...)" in diagnostic.problem_text()
