from typing import Any, cast

import pytest

import structure
from structure import step as dsl_step
from structure import transform, where
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class Raw(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    value = structure.field(structure.Integer(), nullable=True)


class Normalized(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    value = structure.field(structure.Integer(), nullable=True)


class Audited(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    value = structure.field(structure.Integer(), nullable=True)
    audit = structure.field(structure.String(), nullable=True)


class Published(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    value = structure.field(structure.Integer(), nullable=True)
    audit = structure.field(structure.String(), nullable=True)


class DirectNormalize(structure.Transform):
    rows = structure.input(Raw)
    normalized = structure.lane(Normalized)

    @dsl_step(output=normalized)
    def normalize(self, row: Raw) -> Normalized:
        return Normalized(id=row.id, value=row.value)


def test_plain_transform_subclass_compiles_without_class_decorator() -> None:
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id, value=row.value, audit="plain")

    plan = compile_transform(Publish)

    assert plan.name == "Publish"
    assert [item.name for item in plan.inputs] == ["rows"]
    assert [item.name for item in plan.outputs] == ["published"]
    assert [step.name for step in plan.steps] == ["publish"]
    assert plan.options == {}


def test_class_level_decorator_options_do_not_leak_to_undecorated_children() -> None:
    @transform(streaming_compatible=True)
    class StreamingBase(structure.Transform):
        rows = structure.input(Raw)
        normalized = structure.lane(Normalized)

        @dsl_step(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, value=row.value)

    class Publish(StreamingBase):
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="plain")

    assert compile_transform(Publish).options == {}


def test_undecorated_direct_parent_contributes_steps() -> None:
    @transform
    class Publish(DirectNormalize):
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    assert [step.name for step in compile_transform(Publish).steps] == ["normalize", "publish"]


def test_undecorated_indirect_parent_contributes_steps() -> None:
    class Audit(DirectNormalize):
        audited = structure.lane(Audited)

        @dsl_step(output=audited)
        def audit(self, row: Normalized) -> Audited:
            return Audited(id=row.id, value=row.value, audit="audit")

    @transform
    class Publish(Audit):
        published = structure.output(Published)

        def publish(self, row: Audited) -> Published:
            return Published(id=row.id, value=row.value, audit=row.audit)

    assert [step.name for step in compile_transform(Publish).steps] == ["normalize", "audit", "publish"]


def test_multiple_inheritance_runs_parents_in_declared_order() -> None:
    class Audit(structure.Transform):
        audited = structure.lane(Audited)

        @dsl_step(output=audited)
        def audit(self, row: Normalized) -> Audited:
            return Audited(id=row.id, value=row.value, audit="audit")

    @transform
    class Publish(DirectNormalize, Audit):
        published = structure.output(Published)

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
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    assert [step.name for step in compile_transform(Publish).steps] == ["normalize", "publish"]


def test_parent_hooks_attach_to_parent_steps() -> None:
    class NormalizeWithHook(DirectNormalize):
        @structure.raw(inout=structure.lane(DirectNormalize.normalized) | structure.lane(DirectNormalize.normalized))
        def after_normalize(self, *, normalized, spark, ctx):
            return normalized

    @transform
    class Publish(NormalizeWithHook):
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    step = compile_transform(Publish).steps[0]

    assert step.name == "normalize"
    assert [hook.name for hook in step.after_hooks] == ["after_normalize"]
    assert step.origin is not None
    assert step.origin.class_name == "DirectNormalize"
    assert step.after_hooks[0].origin is not None
    assert step.after_hooks[0].origin.class_name == "NormalizeWithHook"


def test_child_hooks_can_target_inherited_parent_steps() -> None:
    @transform
    class Publish(DirectNormalize):
        published = structure.output(Published)

        @structure.raw(inout=structure.lane(DirectNormalize.normalized) | structure.lane(DirectNormalize.normalized))
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
        published = structure.output(Published)

        @dsl_step(output=DirectNormalize.normalized)
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
        published = structure.output(Published)

        @dsl_step(output=DirectNormalize.normalized)
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
    assert [step.origin.class_name if step.origin else None for step in plan.steps] == [
        "DirectNormalize",
        "Publish",
        "Publish",
    ]


def test_override_with_explicit_base_method_schedules_that_parent() -> None:
    @transform
    class Publish(DirectNormalize):
        published = structure.output(Published)

        @dsl_step(output=DirectNormalize.normalized)
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
    class AuditNormalize(structure.Transform):
        normalized = structure.lane(Normalized)

        @dsl_step(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            where(cast(Any, row.id).is_not_null())
            return Normalized(id=row.id, value=row.value)

    @transform
    class Publish(DirectNormalize, AuditNormalize):
        published = structure.output(Published)

        @dsl_step(output=AuditNormalize.normalized)
        def normalize(self, row: Raw) -> Normalized:
            normalized = super(DirectNormalize, self).normalize(row)
            return Normalized(id=normalized.id, value=normalized.value)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    plan = compile_transform(Publish)

    assert [step.name for step in plan.steps] == ["AuditNormalize.normalize", "normalize", "publish"]
    assert len(plan.steps[0].filters) == 1


def test_calling_previous_step_method_directly_fails() -> None:
    @transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        normalized = structure.lane(Normalized)
        published = structure.output(Published)

        @dsl_step(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, value=row.value)

        def publish(self, row: Normalized) -> Published:
            normalized = self.normalize(row)
            return Published(id=normalized.id, value=normalized.value, audit="published")

    with pytest.raises(structure.StructureCompileError, match="Step methods are pipeline steps"):
        compile_transform(Publish)


def test_recursive_step_method_call_fails() -> None:
    @transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return self.publish(row)

    with pytest.raises(structure.StructureCompileError, match="Publish.publish"):
        compile_transform(Publish)


def test_direct_base_method_call_fails_when_it_is_not_an_override_parent_call() -> None:
    @transform
    class Publish(DirectNormalize):
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            normalized = DirectNormalize.normalize(self, row)
            return Published(id=normalized.id, value=normalized.value, audit="published")

    with pytest.raises(structure.StructureCompileError, match="DirectNormalize.normalize"):
        compile_transform(Publish)


def test_direct_unrelated_transform_method_call_fails() -> None:
    class NormalizeElsewhere(structure.Transform):
        rows = structure.input(Raw)
        normalized = structure.output(Normalized)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, value=row.value)

    @transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            normalized = NormalizeElsewhere.normalize(cast(Any, self), row)
            return Published(id=normalized.id, value=normalized.value, audit="published")

    with pytest.raises(structure.StructureCompileError, match="NormalizeElsewhere.normalize"):
        compile_transform(Publish)


def test_public_schema_returning_helper_call_fails() -> None:
    @transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            normalized = self.normalize(row)
            return Published(id=normalized.id, value=normalized.value, audit="published")

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, value=row.value)

    with pytest.raises(structure.StructureCompileError, match="Use source order"):
        compile_transform(Publish)


def test_private_helper_method_remains_allowed() -> None:
    @transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return self._publish(row, "published")

        def _publish(self, row: Raw, audit: str) -> Published:
            return Published(id=row.id, value=row.value, audit=audit)

    assert [step.name for step in compile_transform(Publish).steps] == ["publish"]


def test_special_expr_helper_call_through_self_remains_allowed() -> None:
    @transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        @structure.special(type="expr")
        def clean(value):
            return value

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id), value=row.value, audit="published")

    assert [step.name for step in compile_transform(Publish).steps] == ["publish"]


def test_sibling_duplicate_names_fail_unless_resolved_by_override() -> None:
    class OtherNormalize(structure.Transform):
        normalized = structure.lane(Normalized)

        @dsl_step(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, value=row.value)

    @transform
    class Publish(DirectNormalize, OtherNormalize):
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    with pytest.raises(structure.StructureCompileError, match="normalize"):
        compile_transform(Publish)


def test_generated_pyspark_renders_inherited_and_override_steps_in_order() -> None:
    @transform
    class Publish(DirectNormalize):
        published = structure.output(Published)

        @dsl_step(output=DirectNormalize.normalized)
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

    assert "class DirectNormalizeGenerated:" in text
    assert "class PublishGenerated(DirectNormalizeGenerated):" in text
    assert "    def _step_directnormalize_normalize_0(self, frames):" in text
    assert text.index("# Step method: DirectNormalize.normalize") < text.index("# Step method: normalize")
    assert text.index("# Step method: normalize") < text.index("# Step method: publish")


def test_child_method_with_same_name_overrides_inherited_raw_hook() -> None:
    class NormalizeWithHook(DirectNormalize):
        @structure.raw(inout=structure.lane(DirectNormalize.normalized) | structure.lane(DirectNormalize.normalized))
        def audit(self, *, normalized, spark, ctx):
            return normalized

    @transform
    class Publish(NormalizeWithHook):
        published = structure.output(Published)

        @dsl_step(output=NormalizeWithHook.normalized)
        def normalize(self, row: Raw) -> Normalized:
            normalized = super().normalize(row)
            return Normalized(id=normalized.id, value=normalized.value)

        def audit(self, *, normalized, spark, ctx):
            return normalized

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    recipe = PySpark.plan.lower()(compile_transform(Publish))

    assert not recipe.steps[0].after_hooks
    assert not recipe.steps[1].after_hooks


def test_lowered_recipes_record_step_and_hook_owners() -> None:
    class NormalizeWithHook(DirectNormalize):
        @structure.raw(inout=structure.lane(DirectNormalize.normalized) | structure.lane(DirectNormalize.normalized))
        def after_normalize(self, *, normalized, spark, ctx):
            return normalized

    @transform
    class Publish(NormalizeWithHook):
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    normalize = recipe.steps[0]

    assert normalize.origin is not None
    assert normalize.origin.class_name == "DirectNormalize"
    assert normalize.after_hooks[0].origin is not None
    assert normalize.after_hooks[0].origin.class_name == "NormalizeWithHook"


def test_explicit_parent_step_does_not_run_raw_hook_overridden_by_child_method() -> None:
    class NormalizeWithHook(DirectNormalize):
        @structure.raw(inout=structure.lane(DirectNormalize.normalized) | structure.lane(DirectNormalize.normalized))
        def audit(self, *, normalized, spark, ctx):
            return normalized

    @transform
    class Publish(NormalizeWithHook):
        published = structure.output(Published)

        @dsl_step(output=NormalizeWithHook.normalized)
        def normalize(self, row: Raw) -> Normalized:
            normalized = super().normalize(row)
            return Normalized(id=normalized.id, value=normalized.value)

        def audit(self, *, normalized, spark, ctx):
            return normalized

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    recipe = PySpark.plan.lower()(compile_transform(Publish))

    assert not recipe.steps[0].after_hooks
    assert not recipe.steps[1].after_hooks


def test_online_execution_receives_ordered_plan() -> None:
    @transform
    class Publish(DirectNormalize):
        published = structure.output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, value=row.value, audit="published")

    captured = {}

    def executor(**kwargs):
        captured["steps"] = [step.name for step in kwargs["plan"].steps]
        return object()

    result = Publish(rows=object()).run(structure.StructureSession(schema_types=FakeTypes, online_executor=executor))

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
