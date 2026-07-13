from typing import Any, cast

import pytest

import structure
from structure.app.dsl.api import compile_transform


class Raw(structure.Schema):
    id = structure.field(structure.String(), nullable=False)


class Clean(structure.Schema):
    id = structure.field(structure.String(), nullable=False)


class Published(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    status = structure.field(structure.String(), nullable=False)


class NullableRaw(structure.Schema):
    id = structure.field(structure.String(), nullable=False, primary_key=True)
    optional_id = structure.field(structure.String(), nullable=True)
    amount = structure.field(structure.String(), nullable=True)
    count = structure.field(structure.Integer(), nullable=False)


class Lookup(structure.Schema):
    id = structure.field(structure.String(), nullable=False, primary_key=True)
    group = structure.field(structure.String(), nullable=False)
    label = structure.field(structure.String(), nullable=False)


class Account(structure.Schema):
    id = structure.field(structure.String(), nullable=False, primary_key=True)
    customer_id = structure.field(structure.String(), nullable=False)


class OptionalClean(structure.Schema):
    optional_id = structure.field(structure.String(), nullable=False)


class MoneyClean(structure.Schema):
    amount = structure.field(structure.Decimal(12, 2), nullable=False)
    count = structure.field(structure.Long(), nullable=False)


class FlagClean(structure.Schema):
    is_paid = structure.field(structure.Boolean(), nullable=False)


class LabelClean(structure.Schema):
    label = structure.field(structure.String(), nullable=False)


def test_v1_unsupported_python_boolean_expression_reports_dsl_diagnostic() -> None:
    @structure.transform
    class BadBoolean(structure.Transform):
        rows = structure.input(Raw)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            if row.id:
                return Clean(id=row.id)
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadBoolean)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.docs == "docs/Diagnostics.md#dsl-e0401"
    assert diagnostic.source.endswith("BadBoolean.normalize")
    assert "unsupported symbolic code" in diagnostic.problem_text()
    assert "Structure expression helpers" in diagnostic.use_text()


def test_v1_schema_flow_mismatch_reports_transform_structure_diagnostic() -> None:
    @structure.transform
    class BadFlow(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def normalize(self, row: Raw) -> Clean:
            return Clean(id=row.id)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id, status="ready")

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadFlow)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert diagnostic.context == {"expected": "Raw", "actual": "Clean"}
    assert diagnostic.source.endswith("BadFlow.publish")
    assert "previous step method returns Clean" in diagnostic.problem_text()


def test_v1_missing_output_field_reports_transform_structure_diagnostic() -> None:
    @structure.transform
    class MissingOutput(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(MissingOutput)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert diagnostic.context == {"field": "status", "schema": "Published"}
    assert diagnostic.source.endswith("MissingOutput.publish")
    assert "Published.status is not assigned" in diagnostic.problem_text()


def test_v1_nullable_assignment_to_non_nullable_field_reports_schema_diagnostic() -> None:
    @structure.transform
    class BadNullability(structure.Transform):
        rows = structure.input(NullableRaw)
        clean = structure.output(OptionalClean)

        def normalize(self, row: NullableRaw) -> OptionalClean:
            return OptionalClean(optional_id=row.optional_id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadNullability)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert diagnostic.docs == "docs/Diagnostics.md#schema-e0301"
    assert diagnostic.context == {"field": "optional_id", "schema": "OptionalClean"}
    assert "may produce null" in diagnostic.problem_text()
    assert "where(value.is_not_null())" in diagnostic.use_text()


def test_v1_where_is_not_null_guard_allows_non_nullable_assignment() -> None:
    @structure.transform
    class GuardedNullability(structure.Transform):
        rows = structure.input(NullableRaw)
        clean = structure.output(OptionalClean)

        def normalize(self, row: NullableRaw) -> OptionalClean:
            structure.where(cast(Any, row.optional_id).is_not_null())
            return OptionalClean(optional_id=row.optional_id)

    plan = compile_transform(GuardedNullability)

    assert plan.steps[0].projection[0].field.name == "optional_id"


def test_v1_string_to_decimal_assignment_requires_explicit_conversion() -> None:
    @structure.transform
    class BadConversion(structure.Transform):
        rows = structure.input(NullableRaw)
        clean = structure.output(MoneyClean)

        def normalize(self, row: NullableRaw) -> MoneyClean:
            return MoneyClean(amount=row.amount, count=row.count)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadConversion)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert diagnostic.context == {"field": "amount", "schema": "MoneyClean"}


def test_v1_non_nullable_string_to_decimal_assignment_reports_conversion_diagnostic() -> None:
    class NonNullAmount(structure.Schema):
        amount = structure.field(structure.String(), nullable=False)
        count = structure.field(structure.Integer(), nullable=False)

    @structure.transform
    class BadConversion(structure.Transform):
        rows = structure.input(NonNullAmount)
        clean = structure.output(MoneyClean)

        def normalize(self, row: NonNullAmount) -> MoneyClean:
            return MoneyClean(amount=row.amount, count=row.count)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadConversion)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0302"
    assert diagnostic.docs == "docs/Diagnostics.md#schema-e0302"
    assert diagnostic.context == {"field": "amount", "expected": "Decimal(12, 2)", "actual": "string()"}
    assert "to_decimal" in diagnostic.use_text()


def test_v1_accepted_coercions_compile_without_schema_diagnostics() -> None:
    @structure.transform
    class GoodCoercions(structure.Transform):
        rows = structure.input(NullableRaw)
        clean = structure.output(MoneyClean)

        def normalize(self, row: NullableRaw) -> MoneyClean:
            amount = structure.coalesce(structure.to_decimal(row.amount, precision=12, scale=2), 0)
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
    @structure.transform
    class BadBooleanAssignment(structure.Transform):
        rows = structure.input(NullableRaw)
        clean = structure.output(FlagClean)

        def normalize(self, row: NullableRaw) -> FlagClean:
            return FlagClean(is_paid=row.count)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadBooleanAssignment)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0303"
    assert diagnostic.docs == "docs/Diagnostics.md#schema-e0303"
    assert diagnostic.context == {"field": "is_paid", "expected": "boolean()", "actual": "integer()"}
    assert "value > 0" in diagnostic.use_text()


def test_v1_left_joined_non_nullable_field_is_nullable_until_guarded() -> None:
    @structure.transform
    class BadLeftJoinNullability(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(LabelClean)

        def normalize(self, row: Raw) -> LabelClean:
            item = structure.lookup_join(self.lookup, on=self.lookup.id == row.id, how=structure.Join.LEFT)
            return LabelClean(label=item.label)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadLeftJoinNullability)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert diagnostic.context == {"field": "label", "schema": "LabelClean"}


def test_v1_join_on_primary_key_compiles_without_uniqueness_warning() -> None:
    @structure.transform
    class UniqueJoin(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(self.lookup, on=self.lookup.id == row.id, how=structure.Join.LEFT)
            return Clean(id=row.id)

    plan = compile_transform(UniqueJoin)

    assert [diagnostic.code for diagnostic in plan.diagnostics] == []


def test_v1_join_on_primary_key_accepts_current_row_left_operand() -> None:
    @structure.transform
    class UniqueJoin(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(self.lookup, on=row.id == self.lookup.id, how=structure.Join.LEFT)
            return Clean(id=row.id)

    plan = compile_transform(UniqueJoin)

    assert [diagnostic.code for diagnostic in plan.diagnostics] == []


def test_v1_unproven_lookup_join_key_emits_uniqueness_warning() -> None:
    @structure.transform
    class UnprovenJoin(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(self.lookup, on=self.lookup.group == row.id, how=structure.Join.LEFT)
            return Clean(id=row.id)

    plan = compile_transform(UnprovenJoin)

    assert [diagnostic.code for diagnostic in plan.diagnostics] == ["JOIN-W0601"]
    assert plan.diagnostics[0].docs == "docs/Diagnostics.md#join-w0601"
    assert plan.diagnostics[0].context == {"input": "lookup", "occurrence": "1"}


def test_v1_or_join_condition_reports_join_diagnostic() -> None:
    @structure.transform
    class BadJoinCondition(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(
                self.lookup, on=(self.lookup.id == row.id) | (self.lookup.group == row.id), how=structure.Join.LEFT
            )
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadJoinCondition)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "JOIN-E0601"
    assert diagnostic.docs == "docs/Diagnostics.md#join-e0601"
    assert diagnostic.context == {"input": "lookup", "occurrence": "1"}
    assert "equality key pairs combined with AND" in diagnostic.problem_text()


def test_v1_same_side_join_condition_reports_join_diagnostic() -> None:
    @structure.transform
    class SameSideJoin(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(self.lookup, on=self.lookup.id == self.lookup.group, how=structure.Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(SameSideJoin)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "joined input with the current row" in raised.value.diagnostic.problem_text()


def test_v1_incompatible_join_key_types_report_join_diagnostic() -> None:
    @structure.transform
    class IncompatibleJoin(structure.Transform):
        rows = structure.input(NullableRaw)
        lookup = structure.input(Lookup)
        clean = structure.output(MoneyClean)

        def normalize(self, row: NullableRaw) -> MoneyClean:
            structure.lookup_join(self.lookup, on=self.lookup.id == row.count, how=structure.Join.LEFT)
            return MoneyClean(
                amount=structure.coalesce(structure.to_decimal(row.amount, precision=12, scale=2), 0), count=row.count
            )

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(IncompatibleJoin)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "JOIN-E0601"
    assert "Join key types are incompatible" in diagnostic.problem_text()


def test_v1_inferred_join_without_relation_candidate_reports_diagnostic() -> None:
    @structure.transform
    class MissingRelation(structure.Transform):
        rows = structure.input(Raw)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(on=row.id == row.id, how=structure.Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(MissingRelation)

    diagnostic = raised.value.diagnostic
    assert "Cannot infer joined relation for lookup_join(...)" in diagnostic.problem_text()
    assert "does not reference an unjoined relation" in diagnostic.problem_text()


def test_v1_inferred_join_with_multiple_relation_candidates_reports_diagnostic() -> None:
    @structure.transform
    class MultipleRelations(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        accounts = structure.input(Account)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(on=self.lookup.id == self.accounts.customer_id, how=structure.Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(MultipleRelations)

    diagnostic = raised.value.diagnostic
    assert "Cannot infer joined relation for lookup_join(...)" in diagnostic.problem_text()
    assert "lookup" in diagnostic.problem_text()
    assert "accounts" in diagnostic.problem_text()
    assert "lookup_join(relation=" in diagnostic.problem_text()


def test_v1_inferred_join_with_mixed_composite_candidates_reports_diagnostic() -> None:
    @structure.transform
    class MixedCompositeRelations(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        accounts = structure.input(Account)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(
                on=(row.id == self.lookup.id) & (row.id == self.accounts.customer_id),
                how=structure.Join.LEFT,
            )
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(MixedCompositeRelations)

    diagnostic = raised.value.diagnostic
    assert "Cannot infer joined relation for lookup_join(...)" in diagnostic.problem_text()
    assert "lookup" in diagnostic.problem_text()
    assert "accounts" in diagnostic.problem_text()


def test_v1_inferred_join_self_only_relation_reports_diagnostic() -> None:
    @structure.transform
    class SelfOnlyRelation(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            structure.lookup_join(on=self.lookup.id == self.lookup.group, how=structure.Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(SelfOnlyRelation)

    diagnostic = raised.value.diagnostic
    assert "Each join key pair must compare the inferred joined relation" in diagnostic.problem_text()


def test_v1_member_lookup_join_reports_migration_diagnostic() -> None:
    @structure.transform
    class MemberJoin(structure.Transform):
        rows = structure.input(Raw)
        lookup = structure.input(Lookup)
        clean = structure.output(Clean)

        def normalize(self, row: Raw) -> Clean:
            self.lookup.lookup_join(on=self.lookup.id == row.id, how=structure.Join.LEFT)
            return Clean(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(MemberJoin)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert "self.customers.lookup_join(...) is not supported" in diagnostic.problem_text()
    assert "lookup_join(self.customers, on=...)" in diagnostic.problem_text()
