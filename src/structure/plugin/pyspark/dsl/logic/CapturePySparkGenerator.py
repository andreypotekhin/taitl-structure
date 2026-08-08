from __future__ import annotations

from typing import cast

from structure.dsl import Schema
from structure.plugin.api.v1.model import SymbolicContext
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.operations import OperationPlan, PosexplodeStructPlan, ScalarGeneratorPlan
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    StructureType,
    TimestampType,
    VariantType,
)


class CapturePySparkGenerator:
    """Capture typed row generators into the current symbolic step."""

    def explode_struct(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        scope: str | None = None,
    ) -> RowScope:
        self._validate_options(as_=as_, ordinal=None, scope=scope)
        expression = self._struct_array(value)
        expression_type = cast(ArrayType, expression.type)
        element_type = cast(StructType, expression_type.element)
        self._validate_generated_schema(
            as_,
            element_schema=element_type.schema,
            ordinal=None,
            exact=True,
            outer=False,
            function="explode_struct",
        )
        self._validate_source_collisions(context.default_project_source, generated=as_)

        generated_scope = scope or self._default_scope(as_)
        context.operations.append(
            OperationPlan.explode_struct_operation(
                PosexplodeStructPlan(
                    expression=expression,
                    scope=generated_scope,
                    schema=as_,
                    ordinal=None,
                    function="explode",
                )
            )
        )
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_)

    def explode_outer_struct(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        scope: str | None = None,
    ) -> RowScope:
        self._validate_options(as_=as_, ordinal=None, scope=scope)
        expression = self._struct_array(value)
        expression_type = cast(ArrayType, expression.type)
        element_type = cast(StructType, expression_type.element)
        self._validate_generated_schema(
            as_,
            element_schema=element_type.schema,
            ordinal=None,
            exact=True,
            outer=True,
            function="explode_outer_struct",
        )
        self._validate_source_collisions(context.default_project_source, generated=as_)

        generated_scope = scope or self._default_scope(as_)
        context.operations.append(
            OperationPlan.explode_outer_struct_operation(
                PosexplodeStructPlan(
                    expression=expression,
                    scope=generated_scope,
                    schema=as_,
                    ordinal=None,
                    function="explode_outer",
                    outer=True,
                )
            )
        )
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_)

    def inline_struct(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        scope: str | None = None,
    ) -> RowScope:
        self._validate_options(as_=as_, ordinal=None, scope=scope)
        expression = self._struct_array(value)
        expression_type = cast(ArrayType, expression.type)
        element_type = cast(StructType, expression_type.element)
        self._validate_generated_schema(
            as_,
            element_schema=element_type.schema,
            ordinal=None,
            exact=True,
            outer=False,
            function="inline_struct",
        )
        self._validate_source_collisions(context.default_project_source, generated=as_)

        generated_scope = scope or self._default_scope(as_)
        context.operations.append(
            OperationPlan.inline_struct_operation(
                PosexplodeStructPlan(
                    expression=expression,
                    scope=generated_scope,
                    schema=as_,
                    ordinal=None,
                    function="inline",
                )
            )
        )
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_)

    def inline_outer_struct(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        scope: str | None = None,
    ) -> RowScope:
        self._validate_options(as_=as_, ordinal=None, scope=scope)
        expression = self._struct_array(value)
        expression_type = cast(ArrayType, expression.type)
        element_type = cast(StructType, expression_type.element)
        self._validate_generated_schema(
            as_,
            element_schema=element_type.schema,
            ordinal=None,
            exact=True,
            outer=True,
            function="inline_outer_struct",
        )
        self._validate_source_collisions(context.default_project_source, generated=as_)

        generated_scope = scope or self._default_scope(as_)
        context.operations.append(
            OperationPlan.inline_outer_struct_operation(
                PosexplodeStructPlan(
                    expression=expression,
                    scope=generated_scope,
                    schema=as_,
                    ordinal=None,
                    function="inline_outer",
                    outer=True,
                )
            )
        )
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_)

    def posexplode_struct(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        ordinal: str = "ordinal",
        scope: str | None = None,
    ) -> RowScope:
        self._validate_options(as_=as_, ordinal=ordinal, scope=scope)
        expression = self._struct_array(value)
        expression_type = cast(ArrayType, expression.type)
        element_type = cast(StructType, expression_type.element)
        self._validate_generated_schema(
            as_,
            element_schema=element_type.schema,
            ordinal=ordinal,
            exact=False,
            outer=False,
            function="posexplode_struct",
        )
        self._validate_source_collisions(context.default_project_source, generated=as_)

        generated_scope = scope or self._default_scope(as_)
        context.operations.append(
            OperationPlan.posexplode_struct_operation(
                PosexplodeStructPlan(
                    expression=expression,
                    scope=generated_scope,
                    schema=as_,
                    ordinal=ordinal,
                )
            )
        )
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_)

    def posexplode_outer_struct(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        ordinal: str = "ordinal",
        scope: str | None = None,
    ) -> RowScope:
        self._validate_options(as_=as_, ordinal=ordinal, scope=scope)
        expression = self._struct_array(value)
        expression_type = cast(ArrayType, expression.type)
        element_type = cast(StructType, expression_type.element)
        self._validate_generated_schema(
            as_,
            element_schema=element_type.schema,
            ordinal=ordinal,
            exact=False,
            outer=True,
            function="posexplode_outer_struct",
        )
        self._validate_source_collisions(context.default_project_source, generated=as_)

        generated_scope = scope or self._default_scope(as_)
        context.operations.append(
            OperationPlan.posexplode_outer_struct_operation(
                PosexplodeStructPlan(
                    expression=expression,
                    scope=generated_scope,
                    schema=as_,
                    ordinal=ordinal,
                    function="posexplode_outer",
                    outer=True,
                )
            )
        )
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_)

    def explode_array(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        value_field: str,
        scope: str | None = None,
    ) -> RowScope:
        return self._scalar_array(
            context, value, as_=as_, value_field=value_field, ordinal=None, scope=scope, outer=False
        )

    def explode_outer_array(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        value_field: str,
        scope: str | None = None,
    ) -> RowScope:
        return self._scalar_array(
            context, value, as_=as_, value_field=value_field, ordinal=None, scope=scope, outer=True
        )

    def posexplode_array(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        value_field: str,
        ordinal: str = "ordinal",
        scope: str | None = None,
    ) -> RowScope:
        return self._scalar_array(
            context, value, as_=as_, value_field=value_field, ordinal=ordinal, scope=scope, outer=False
        )

    def posexplode_outer_array(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        value_field: str,
        ordinal: str = "ordinal",
        scope: str | None = None,
    ) -> RowScope:
        return self._scalar_array(
            context, value, as_=as_, value_field=value_field, ordinal=ordinal, scope=scope, outer=True
        )

    def _scalar_array(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        value_field: str,
        ordinal: str | None,
        scope: str | None,
        outer: bool,
    ) -> RowScope:
        function = ("posexplode" if ordinal is not None else "explode") + ("_outer" if outer else "") + "_array"
        self._validate_options(as_=as_, ordinal=ordinal, scope=scope, function=function)
        if not isinstance(value_field, str) or not value_field:
            raise TypeError(f"{function}(value_field=...) requires a non-empty field name")
        expression = self._scalar_array_expression(value, function=function, outer=outer)
        self._validate_scalar_schema(
            as_,
            value_field=value_field,
            ordinal=ordinal,
            element=cast(ArrayType, expression.type).element,
            outer=outer,
            function=function,
        )
        self._validate_source_collisions(context.default_project_source, generated=as_)
        generated_scope = scope or self._default_scope(as_)
        generator = ScalarGeneratorPlan(
            expression=expression,
            scope=generated_scope,
            schema=as_,
            value_field=value_field,
            ordinal=ordinal,
            function=function.removesuffix("_array"),
            outer=outer,
        )
        operation = getattr(OperationPlan, f"{generator.function}_array_operation")
        context.operations.append(operation(generator))
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_, nullable=outer)

    def _scalar_array_expression(self, value: object, *, function: str, outer: bool) -> Expression:
        expression = literal(value)
        if not isinstance(expression, Expression) or not isinstance(expression.type, ArrayType):
            raise TypeError(f"{function}(...) requires an array of primitive scalar values")
        if not isinstance(expression.type.element, self._scalar_array_types()):
            raise TypeError(f"{function}(...) requires an array of primitive scalar values")
        if not outer and expression.nullable:
            raise TypeError(f"{function}(...) requires a non-nullable array expression")
        if not outer and expression.type.contains_null:
            raise TypeError(f"{function}(...) requires contains_null=False for array elements")
        return expression

    @staticmethod
    def _scalar_array_types() -> tuple[type[StructureType], ...]:
        return (
            StringType,
            BooleanType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType,
            DateType,
            TimestampType,
            BinaryType,
        )

    def _validate_scalar_schema(
        self,
        schema: type[Schema],
        *,
        value_field: str,
        ordinal: str | None,
        element: StructureType,
        outer: bool,
        function: str,
    ) -> None:
        fields = schema._structure_fields
        allowed = {value_field}
        if value_field not in fields:
            raise TypeError(f"{function}(as_=...) schema must declare value field {value_field!r}")
        value = fields[value_field]
        if not self._same_type(value.type, element):
            raise TypeError(f"{function}(as_=...) field {value_field!r} must match the array element type")
        if outer and not value.nullable:
            raise TypeError(f"{function}(as_=...) field {value_field!r} must be nullable for outer generator rows")
        if not outer and value.nullable:
            raise TypeError(f"{function}(as_=...) field {value_field!r} must be non-nullable")
        if ordinal is not None:
            allowed.add(ordinal)
            if ordinal == value_field:
                raise TypeError(f"{function}(ordinal=...) must differ from value_field")
            if ordinal not in fields:
                raise TypeError(f"{function}(as_=...) schema must declare ordinal field {ordinal!r}")
            if not isinstance(fields[ordinal].type, LongType):
                raise TypeError(f"{function}(ordinal={ordinal!r}) field must be long()")
            if outer and not fields[ordinal].nullable:
                raise TypeError(f"{function}(ordinal={ordinal!r}) field must be nullable for outer generator rows")
            if not outer and fields[ordinal].nullable:
                raise TypeError(f"{function}(ordinal={ordinal!r}) field must be non-nullable")
        extras = sorted(set(fields) - allowed)
        if extras:
            raise TypeError(f"{function}(as_=...) schema contains undeclared generated field(s): {', '.join(extras)}")

    @staticmethod
    def _same_type(left: StructureType, right: StructureType) -> bool:
        if left.name != right.name:
            return False
        if isinstance(left, DecimalType) and isinstance(right, DecimalType):
            return left.precision == right.precision and left.scale == right.scale
        return True

    def variant_explode(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        scope: str | None = None,
    ) -> RowScope:
        return self._variant_explode(context, value, as_=as_, scope=scope, outer=False)

    def variant_explode_outer(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        scope: str | None = None,
    ) -> RowScope:
        return self._variant_explode(context, value, as_=as_, scope=scope, outer=True)

    def _variant_explode(
        self,
        context: SymbolicContext,
        value: object,
        *,
        as_: type[Schema],
        scope: str | None,
        outer: bool,
    ) -> RowScope:
        function = "variant_explode_outer" if outer else "variant_explode"
        self._validate_options(as_=as_, ordinal=None, scope=scope)
        expression = literal(value)
        if not isinstance(expression, Expression) or not isinstance(expression.type, VariantType):
            raise TypeError(f"{function}(...) requires a Variant expression")
        self._validate_variant_schema(as_, outer=outer, function=function)
        self._validate_source_collisions(context.default_project_source, generated=as_)

        generated_scope = scope or self._default_scope(as_)
        operation = OperationPlan.variant_explode_outer_operation if outer else OperationPlan.variant_explode_operation
        context.operations.append(
            operation(
                PosexplodeStructPlan(
                    expression=expression,
                    scope=generated_scope,
                    schema=as_,
                    ordinal=None,
                    function=function,
                    outer=outer,
                    tvf=True,
                )
            )
        )
        context.register_current_scope(generated_scope)
        return RowScope(name=generated_scope, schema=as_, nullable=outer)

    def _validate_options(
        self,
        *,
        as_: type[Schema],
        ordinal: str | None,
        scope: str | None,
        function: str = "posexplode_struct",
    ) -> None:
        if not isinstance(as_, type) or not issubclass(as_, Schema):
            raise TypeError(f"{function}(as_=...) requires a Structure Schema class")
        if ordinal is not None and (not isinstance(ordinal, str) or not ordinal):
            raise TypeError(f"{function}(ordinal=...) requires a non-empty field name")
        if scope is not None and (not isinstance(scope, str) or not scope):
            raise TypeError(f"{function}(scope=...) requires a non-empty string")

    def _struct_array(self, value: object) -> Expression:
        expression = literal(value)
        if not isinstance(expression, Expression) or not isinstance(expression.type, ArrayType):
            raise TypeError("posexplode_struct(...) requires an array<struct<...>> Structure expression")
        if not isinstance(expression.type.element, StructType):
            raise TypeError("posexplode_struct(...) requires an array<struct<...>> Structure expression")
        if expression.type.contains_null:
            raise TypeError(
                "posexplode_struct(...) requires contains_null=False until null element semantics are admitted"
            )
        return expression

    def _validate_generated_schema(
        self,
        schema: type[Schema],
        *,
        element_schema: type[Schema],
        ordinal: str | None,
        exact: bool,
        outer: bool,
        function: str,
    ) -> None:
        fields = schema._structure_fields
        if ordinal is not None and ordinal not in fields:
            raise TypeError(f"posexplode_struct(as_=...) schema must declare ordinal field {ordinal!r}")
        if ordinal is not None and not isinstance(fields[ordinal].type, LongType):
            raise TypeError(f"posexplode_struct(ordinal={ordinal!r}) field must be long()")
        if ordinal is not None and outer and not fields[ordinal].nullable:
            raise TypeError(f"{function}(ordinal={ordinal!r}) field must be nullable for outer generator rows")
        for name, field in element_schema._structure_fields.items():
            if name not in fields:
                raise TypeError(f"posexplode_struct(as_=...) schema must declare element field {name!r}")
            if fields[name].type != field.type:
                raise TypeError(f"posexplode_struct(as_=...) field {name!r} must match the array element field type")
            if outer and not fields[name].nullable:
                raise TypeError(f"{function}(as_=...) field {name!r} must be nullable for outer generator rows")
        allowed = set(element_schema._structure_fields)
        if ordinal is not None:
            allowed.add(ordinal)
        extras = sorted(set(fields) - allowed)
        if exact and extras:
            raise TypeError(
                "explode_struct(as_=...) schema must contain exactly the array element fields; "
                f"extra field(s): {', '.join(extras)}"
            )

    def _validate_source_collisions(self, source: object, *, generated: type[Schema]) -> None:
        source_schema = getattr(source, "_structure_scope_schema", None)
        if not isinstance(source_schema, type) or not issubclass(source_schema, Schema):
            return
        source_schema = cast(type[Schema], source_schema)
        source_columns = {field.column for field in source_schema._structure_fields.values()}
        generated_columns = {field.column for field in generated._structure_fields.values()}
        collisions = sorted(source_columns & generated_columns)
        if collisions:
            raise TypeError(
                "posexplode_struct(as_=...) generated columns collide with current input column(s): "
                f"{', '.join(collisions)}. Use field aliases on the generated schema."
            )

    def _validate_variant_schema(self, schema: type[Schema], *, outer: bool, function: str) -> None:
        expected = {"pos": LongType, "key": StringType, "value": VariantType}
        fields = schema._structure_fields
        missing = sorted(set(expected) - set(fields))
        extras = sorted(set(fields) - set(expected))
        if missing or extras:
            detail = []
            if missing:
                detail.append(f"missing field(s): {', '.join(missing)}")
            if extras:
                detail.append(f"extra field(s): {', '.join(extras)}")
            raise TypeError(f"{function}(as_=...) requires exactly pos, key, and value fields ({'; '.join(detail)})")
        for name, expected_type in expected.items():
            if fields[name].type.name != expected_type().name:
                raise TypeError(f"{function}(as_=...) field {name!r} must have type {expected_type.__name__}")
        if outer:
            non_nullable = sorted(name for name, field in fields.items() if not field.nullable)
            if non_nullable:
                raise TypeError(
                    f"{function}(as_=...) fields must be nullable for the outer null row: {', '.join(non_nullable)}"
                )

    def _default_scope(self, schema: type[Schema]) -> str:
        name = schema.__name__
        return name[:1].lower() + name[1:]
