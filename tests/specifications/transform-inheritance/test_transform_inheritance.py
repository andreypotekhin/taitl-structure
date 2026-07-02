from typing import Any, cast

import pytest

from structure import (
    Integer,
    String,
    Structure,
    StructureCompileError,
    StructureSession,
    Transform,
    after,
    field,
    input,
    lane,
    output,
    transform,
    where,
)
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class Raw(Structure):
    id = field(String(), nullable=False)
    value = field(Integer(), nullable=True)


class Normalized(Structure):
    id = field(String(), nullable=False)
    value = field(Integer(), nullable=True)


class Audited(Structure):
    id = field(String(), nullable=False)
    value = field(Integer(), nullable=True)
    audit = field(String(), nullable=True)


class Published(Structure):
    id = field(String(), nullable=False)
    value = field(Integer(), nullable=True)
    audit = field(String(), nullable=True)


class DirectNormalize(Transform):
    rows = input(Raw)
    normalized = lane(Normalized)

    @transform(output=normalized)
    def normalize(self, row: Raw) -> Normalized:
        return Normalized(id=row.id, value=row.value)


def test_undecorated_direct_parent_contributes_steps() -> None:
    @transform
    class Publish(DirectNormalize):
        published = output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    assert [step.name for step in compile_transform(Publish).steps] == ["normalize", "publish"]


def test_undecorated_indirect_parent_contributes_steps() -> None:
    class Audit(DirectNormalize):
        audited = lane(Audited)

        @transform(output=audited)
        def audit(self, row: Normalized) -> Audited:
            return Audited(id=row.id, value=row.value, audit="audit")

    @transform
    class Publish(Audit):
        published = output(Published)

        def publish(self, row: Audited) -> Published:
            return Published(id=row.id, value=row.value, audit=row.audit)

    assert [step.name for step in compile_transform(Publish).steps] == ["normalize", "audit", "publish"]


def test_multiple_inheritance_runs_parents_in_declared_order() -> None:
    class Audit(Transform):
        audited = lane(Audited)

        @transform(output=audited)
        def audit(self, row: Normalized) -> Audited:
            return Audited(id=row.id, value=row.value, audit="audit")

    @transform
    class Publish(DirectNormalize, Audit):
        published = output(Published)

        def publish(self, row: Audited) -> Published:
            return Published(id=row.id, value=row.value, audit=row.audit)

    assert [step.name for step in compile_transform(Publish).steps] == ["normalize", "audit", "publish"]


def test_diamond_ancestor_contributes_once() -> None:
    class Left(DirectNormalize):
        pass

    class Right(DirectNormalize):
        pass

    @transform
    class Publish(Left, Right):
        published = output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    assert [step.name for step in compile_transform(Publish).steps] == ["normalize", "publish"]


def test_parent_hooks_attach_to_parent_steps() -> None:
    class NormalizeWithHook(DirectNormalize):
        @after(DirectNormalize.normalize, lane=DirectNormalize.normalized)
        def after_normalize(self, *, normalized, spark, ctx):
            return normalized

    @transform
    class Publish(NormalizeWithHook):
        published = output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    step = compile_transform(Publish).steps[0]

    assert step.name == "normalize"
    assert [hook.name for hook in step.after_hooks] == ["after_normalize"]


def test_child_hooks_can_target_inherited_parent_steps() -> None:
    @transform
    class Publish(DirectNormalize):
        published = output(Published)

        @after(DirectNormalize.normalize, lane=DirectNormalize.normalized)
        def after_normalize(self, *, normalized, spark, ctx):
            return normalized

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    step = compile_transform(Publish).steps[0]

    assert step.name == "normalize"
    assert [hook.name for hook in step.after_hooks] == ["after_normalize"]


def test_override_without_parent_call_replaces_inherited_step_position() -> None:
    @transform
    class Publish(DirectNormalize):
        published = output(Published)

        @transform(output=DirectNormalize.normalized)
        def normalize(self, row: Raw) -> Normalized:
            where(cast(Any, row.value).is_not_null())
            return Normalized(id=row.id, value=row.value)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    plan = compile_transform(Publish)

    assert [step.name for step in plan.steps] == ["normalize", "publish"]
    assert len(plan.steps[0].filters) == 1


def test_override_with_zero_arg_super_schedules_parent_before_child() -> None:
    @transform
    class Publish(DirectNormalize):
        published = output(Published)

        @transform(output=DirectNormalize.normalized)
        def normalize(self, row: Raw) -> Normalized:
            normalized = super().normalize(row)
            where(cast(Any, normalized.value).is_not_null())
            return Normalized(id=normalized.id, value=normalized.value)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    plan = compile_transform(Publish)

    assert [step.name for step in plan.steps] == ["DirectNormalize.normalize", "normalize", "publish"]
    assert not plan.steps[0].filters
    assert len(plan.steps[1].filters) == 1


def test_override_with_explicit_base_method_schedules_that_parent() -> None:
    @transform
    class Publish(DirectNormalize):
        published = output(Published)

        @transform(output=DirectNormalize.normalized)
        def normalize(self, row: Raw) -> Normalized:
            normalized = DirectNormalize.normalize(self, row)
            return Normalized(id=normalized.id, value=normalized.value)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    assert [step.name for step in compile_transform(Publish).steps] == [
        "DirectNormalize.normalize",
        "normalize",
        "publish",
    ]


def test_override_with_two_arg_super_schedules_next_mro_parent() -> None:
    class AuditNormalize(Transform):
        normalized = lane(Normalized)

        @transform(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            where(cast(Any, row.id).is_not_null())
            return Normalized(id=row.id, value=row.value)

    @transform
    class Publish(DirectNormalize, AuditNormalize):
        published = output(Published)

        @transform(output=AuditNormalize.normalized)
        def normalize(self, row: Raw) -> Normalized:
            normalized = super(DirectNormalize, self).normalize(row)
            return Normalized(id=normalized.id, value=normalized.value)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    plan = compile_transform(Publish)

    assert [step.name for step in plan.steps] == ["AuditNormalize.normalize", "normalize", "publish"]
    assert len(plan.steps[0].filters) == 1


def test_sibling_duplicate_names_fail_unless_resolved_by_override() -> None:
    class OtherNormalize(Transform):
        normalized = lane(Normalized)

        @transform(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, value=row.value)

    @transform
    class Publish(DirectNormalize, OtherNormalize):
        published = output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    with pytest.raises(StructureCompileError, match="normalize"):
        compile_transform(Publish)


def test_generated_pyspark_renders_inherited_and_override_steps_in_order() -> None:
    @transform
    class Publish(DirectNormalize):
        published = output(Published)

        @transform(output=DirectNormalize.normalized)
        def normalize(self, row: Raw) -> Normalized:
            normalized = super().normalize(row)
            return Normalized(id=normalized.id, value=normalized.value)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(Publish)),
        source_transform=f"{__name__}.Publish",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules={Raw: __name__, Normalized: __name__, Published: __name__},
    )

    assert text.index("# Subtransform: DirectNormalize.normalize") < text.index("# Subtransform: normalize")
    assert text.index("# Subtransform: normalize") < text.index("# Subtransform: publish")


def test_online_execution_receives_ordered_plan() -> None:
    @transform
    class Publish(DirectNormalize):
        published = output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    captured = {}

    def executor(**kwargs):
        captured["steps"] = [step.name for step in kwargs["plan"].steps]
        return object()

    result = Publish(rows=object()).run(StructureSession(schema_types=FakeTypes, online_executor=executor))

    assert result.published is not None
    assert captured["steps"] == ["normalize", "publish"]


class FakeTypes:
    @staticmethod
    def StructType(fields):
        return ("StructType", tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return ("StructField", name, dataType, nullable)

    @staticmethod
    def StringType():
        return "StringType"

    @staticmethod
    def IntegerType():
        return "IntegerType"
