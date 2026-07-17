from __future__ import annotations

import builtins
from dataclasses import dataclass
from typing import Any, Mapping, cast

from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DateType import DateType
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.FloatType import FloatType
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.dsl.model.types.TimestampType import TimestampType

_ORDERABLE_TYPES = frozenset({"date", "decimal", "double", "float", "integer", "long", "string", "timestamp"})


def _schema_field(schema: Any, name: str):
    fields = schema._structure_fields
    return fields.get(name) or next((field for field in fields.values() if field.column == name), None)


def _same_field(left, right) -> bool:
    return (
        left.name == right.name
        and left.column == right.column
        and left.nullable == right.nullable
        and _same_type(left.type, right.type)
    )


def _same_type(left: StructureType | None, right: StructureType | None) -> bool:
    if left is None or right is None or left.name != right.name:
        return False
    if isinstance(left, DecimalType) and isinstance(right, DecimalType):
        return left.precision == right.precision and left.scale == right.scale
    if isinstance(left, ArrayType) and isinstance(right, ArrayType):
        return left.contains_null == right.contains_null and _same_type(left.element, right.element)
    if isinstance(left, MapType) and isinstance(right, MapType):
        return (
            left.value_contains_null == right.value_contains_null
            and _same_type(left.key, right.key)
            and _same_type(left.value, right.value)
        )
    return not isinstance(left, StructType) or not isinstance(right, StructType) or left.schema is right.schema


@dataclass(frozen=True, eq=False)
class Expression:
    kind: str
    type: StructureType | None = None
    nullable: bool = True
    data: Mapping[str, object] | None = None
    args: tuple["Expression", ...] = ()

    def is_null(self) -> "Expression":
        return Expression(kind="is_null", type=BooleanType(), nullable=False, args=(self,))

    def is_not_null(self) -> "Expression":
        return Expression(kind="is_not_null", type=BooleanType(), nullable=False, args=(self,))

    def null_safe_eq(self, other: object) -> "Expression":
        return self._comparison("null_safe_eq", other, nullable=False)

    def isin(self, *values: object) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        if not values:
            raise TypeError("isin(...) requires at least one value")
        arguments = tuple(literal(value) for value in values)
        if any(not self._comparison_compatible(argument) for argument in arguments):
            raise TypeError("isin(...) requires values compatible with its expression type")
        return Expression(
            kind="isin",
            type=BooleanType(),
            nullable=self.nullable or any(argument.nullable for argument in arguments),
            args=(self, *arguments),
        )

    def between(self, lower: object, upper: object) -> "Expression":
        return (self >= lower) & (self <= upper)

    def contains(self, value: str) -> "Expression":
        return self._string_predicate("contains", value)

    def startswith(self, prefix: str) -> "Expression":
        return self._string_predicate("startswith", prefix)

    def endswith(self, suffix: str) -> "Expression":
        return self._string_predicate("endswith", suffix)

    def like(self, pattern: str) -> "Expression":
        return self._string_predicate("like", pattern)

    def ilike(self, pattern: str) -> "Expression":
        return self._string_predicate("ilike", pattern)

    def rlike(self, pattern: str) -> "Expression":
        return self._string_predicate("rlike", pattern)

    def cast(self, target: StructureType) -> "Expression":
        return self._cast(target)

    def astype(self, target: StructureType) -> "Expression":
        return self._cast(target)

    def try_cast(self, target: StructureType) -> "Expression":
        cast_expression = self._cast(target)
        return Expression(
            kind="try_cast",
            type=target,
            nullable=True,
            data=cast_expression.data,
            args=cast_expression.args,
        )

    def asc(self) -> "Expression":
        return self._order("asc")

    def desc(self) -> "Expression":
        return self._order("desc")

    def asc_nulls_first(self) -> "Expression":
        return self._order("asc_nulls_first")

    def asc_nulls_last(self) -> "Expression":
        return self._order("asc_nulls_last")

    def desc_nulls_first(self) -> "Expression":
        return self._order("desc_nulls_first")

    def desc_nulls_last(self) -> "Expression":
        return self._order("desc_nulls_last")

    def __getitem__(self, key: object) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        item = literal(key)
        if isinstance(self.type, ArrayType):
            if item.type is None or item.type.name not in {"integer", "long"}:
                raise TypeError("Array indexing requires an integral Structure expression")
            return Expression(kind="item", type=self.type.element, nullable=True, args=(self, item))
        if isinstance(self.type, MapType):
            if item.type is None or item.type.name != self.type.key.name:
                raise TypeError("Map indexing requires a key with the map key type")
            return Expression(kind="item", type=self.type.value, nullable=True, args=(self, item))
        raise TypeError("Indexing requires an Array or Map Structure expression")

    def __getattr__(self, name: str) -> "Expression":
        return self._struct_field(name, attribute=True)

    def get_field(self, name: str) -> "Expression":
        if not isinstance(name, str) or not name:
            raise TypeError("get_field(...) requires a non-empty field name")
        return self._struct_field(name, attribute=False)

    def with_field(self, name: str, value: object, *, schema: Any) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        source, target = self._struct_mutation_schema(schema, "with_field(...)")
        field = _schema_field(target, name)
        if field is None:
            raise TypeError(f"with_field(...) result schema {target.__name__} cannot find {name!r}")
        replacement = literal(value)
        if not _same_type(replacement.type, field.type) or replacement.nullable and not field.nullable:
            raise TypeError("with_field(...) value must match the declared result field type and nullability")
        source_fields = source._structure_fields
        target_fields = target._structure_fields
        expected = set(source_fields) | {field.name}
        if set(target_fields) != expected or any(
            name != field.name and not _same_field(source_fields[name], target_fields[name]) for name in source_fields
        ):
            raise TypeError("with_field(...) result schema must preserve every source field except the replaced field")
        return Expression(
            kind="with_field",
            type=StructType(target),
            nullable=self.nullable,
            data={"field": field.column},
            args=(self, replacement),
        )

    def drop_fields(self, *names: str, schema: Any) -> "Expression":
        source, target = self._struct_mutation_schema(schema, "drop_fields(...)")
        if not names or any(not isinstance(name, str) or not name for name in names):
            raise TypeError("drop_fields(...) requires at least one non-empty field name")
        source_fields = source._structure_fields
        dropped = tuple(_schema_field(source, name) for name in names)
        if any(field is None for field in dropped):
            raise TypeError("drop_fields(...) names must exist in the source Struct schema")
        dropped_names = {field.name for field in dropped if field is not None}
        remaining = {name: field for name, field in source_fields.items() if name not in dropped_names}
        if set(target._structure_fields) != set(remaining) or any(
            not _same_field(field, target._structure_fields[name]) for name, field in remaining.items()
        ):
            raise TypeError("drop_fields(...) result schema must equal the source schema without the dropped fields")
        return Expression(
            kind="drop_fields",
            type=StructType(target),
            nullable=self.nullable,
            data={"fields": tuple(field.column for field in dropped if field is not None)},
            args=(self,),
        )

    def _struct_field(self, name: str, *, attribute: bool) -> "Expression":
        if not isinstance(self.type, StructType):
            if attribute:
                raise AttributeError(name)
            raise TypeError("get_field(...) requires a Struct Structure expression")
        fields = self.type.schema._structure_fields
        field = fields.get(name) or next((item for item in fields.values() if item.column == name), None)
        if field is None:
            if attribute:
                raise AttributeError(name)
            raise TypeError(f"get_field(...) cannot find {name!r} in {self.type.schema.__name__}")

        if attribute:
            data = dict(self.data or {})
            path_data = data.get("path")
            path = cast(tuple[object, ...], path_data) if isinstance(path_data, tuple) else (data.get("field"),)
            name_path_data = data.get("name_path")
            name_path = (
                cast(tuple[object, ...], name_path_data) if isinstance(name_path_data, tuple) else (data.get("name"),)
            )
            path_strings = tuple(str(item) for item in path if item)
            name_path_strings = tuple(str(item) for item in name_path if item)
            data["field"] = ".".join((*path_strings, field.column))
            data["field_nullable"] = field.nullable
            data["name"] = ".".join((*name_path_strings, field.name))
            data["path"] = (*path_strings, field.column)
            data["name_path"] = (*name_path_strings, field.name)
            return Expression(kind="field", type=field.type, nullable=self.nullable or field.nullable, data=data)

        return Expression(
            kind="get_field",
            type=field.type,
            nullable=self.nullable or field.nullable,
            data={"field": field.column, "name": field.name},
            args=(self,),
        )

    def _struct_mutation_schema(self, schema: Any, call: str) -> tuple[Any, Any]:
        if not isinstance(self.type, StructType):
            raise TypeError(f"{call} requires a Struct Structure expression")
        if not isinstance(schema, builtins.type) or not hasattr(schema, "_structure_fields"):
            raise TypeError(f"{call} schema must be a declared Structure Schema class")
        return self.type.schema, schema

    def __and__(self, other: object) -> "Expression":
        return self._boolean_binary("and", other)

    def __or__(self, other: object) -> "Expression":
        return self._boolean_binary("or", other)

    def __invert__(self) -> "Expression":
        self._require_boolean("~")
        return Expression(kind="not", type=BooleanType(), nullable=self.nullable, args=(self,))

    def __eq__(self, other: object) -> "Expression":  # type: ignore[override]
        return self._comparison("eq", other)

    def __ne__(self, other: object) -> "Expression":  # type: ignore[override]
        return self._comparison("ne", other)

    def __add__(self, other: object) -> "Expression":
        return self._arithmetic("add", other)

    def __radd__(self, other: object) -> "Expression":
        return self._arithmetic("add", other, reverse=True)

    def __sub__(self, other: object) -> "Expression":
        return self._arithmetic("sub", other)

    def __rsub__(self, other: object) -> "Expression":
        return self._arithmetic("sub", other, reverse=True)

    def __mul__(self, other: object) -> "Expression":
        return self._arithmetic("mul", other)

    def __rmul__(self, other: object) -> "Expression":
        return self._arithmetic("mul", other, reverse=True)

    def __truediv__(self, other: object) -> "Expression":
        return self._arithmetic("div", other)

    def __rtruediv__(self, other: object) -> "Expression":
        return self._arithmetic("div", other, reverse=True)

    def __mod__(self, other: object) -> "Expression":
        return self._arithmetic("mod", other)

    def __rmod__(self, other: object) -> "Expression":
        return self._arithmetic("mod", other, reverse=True)

    def __neg__(self) -> "Expression":
        self._arithmetic_type("neg", self)
        return Expression(kind="neg", type=self.type, nullable=self.nullable, args=(self,))

    def bitwise_and(self, other: object) -> "Expression":
        return self._bitwise_binary("bitwise_and", other)

    def bitwise_or(self, other: object) -> "Expression":
        return self._bitwise_binary("bitwise_or", other)

    def bitwise_xor(self, other: object) -> "Expression":
        return self._bitwise_binary("bitwise_xor", other)

    def bitwise_not(self) -> "Expression":
        self._require_integral()
        return Expression(kind="bitwise_not", type=self.type, nullable=self.nullable, args=(self,))

    def __gt__(self, other: object) -> "Expression":
        return self._comparison("gt", other)

    def __lt__(self, other: object) -> "Expression":
        return self._comparison("lt", other)

    def __le__(self, other: object) -> "Expression":
        return self._comparison("le", other)

    def __ge__(self, other: object) -> "Expression":
        return self._comparison("ge", other)

    def __bool__(self) -> bool:
        raise TypeError("Structure expressions cannot be used as Python booleans. Use where(...), &, |, or ~.")

    def _binary(
        self,
        kind: str,
        other: object,
        *,
        type: StructureType | None = None,
        nullable: bool = False,
    ) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        return Expression(kind=kind, type=type, nullable=nullable, args=(self, literal(other)))

    def _reverse_binary(
        self,
        kind: str,
        other: object,
        *,
        type: StructureType | None = None,
        nullable: bool = False,
    ) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        return Expression(kind=kind, type=type, nullable=nullable, args=(literal(other), self))

    def _boolean_binary(self, kind: str, other: object) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        other_expression = literal(other)
        self._require_boolean(kind)
        if not isinstance(other_expression.type, BooleanType):
            raise TypeError(f"{kind}(...) requires Boolean Structure expressions")
        return Expression(
            kind=kind,
            type=BooleanType(),
            nullable=self.nullable or other_expression.nullable,
            args=(self, other_expression),
        )

    def _comparison(self, kind: str, other: object, *, nullable: bool | None = None) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        other_expression = literal(other)
        problem = self._comparison_problem(kind, other_expression)
        return Expression(
            kind=kind,
            type=BooleanType(),
            nullable=(self.nullable or other_expression.nullable) if nullable is None else nullable,
            data={"comparison_problem": problem} if problem is not None else None,
            args=(self, other_expression),
        )

    def _comparison_problem(self, kind: str, other: "Expression") -> str | None:
        if not self._comparison_compatible(other):
            return "Comparison requires compatible Structure expression types"
        if kind in {"gt", "lt", "le", "ge"} and not self._orderable_comparison(other):
            return "Ordering comparisons require orderable Structure expression types"
        return None

    def _comparison_compatible(self, other: "Expression") -> bool:
        if isinstance(self.type, MapType) or isinstance(other.type, MapType):
            return False
        if self.type is None or other.type is None:
            return (self.type is None and self.nullable) or (other.type is None and other.nullable)
        return self._compatible_comparison_types(self.type, other.type)

    def _compatible_comparison_types(self, left: StructureType, right: StructureType) -> bool:
        if isinstance(left, (IntegerType, LongType, FloatType, DoubleType, DecimalType)) and isinstance(
            right, (IntegerType, LongType, FloatType, DoubleType, DecimalType)
        ):
            return True
        if isinstance(left, (DateType, TimestampType)) and isinstance(right, (DateType, TimestampType)):
            return True
        if left.name != right.name:
            return False
        if isinstance(left, ArrayType) and isinstance(right, ArrayType):
            return self._compatible_comparison_types(left.element, right.element)
        if isinstance(left, StructType) and isinstance(right, StructType):
            return left.schema is right.schema
        return True

    def _orderable_comparison(self, other: "Expression") -> bool:
        if self.type is None or other.type is None:
            return True
        return self.type.name in _ORDERABLE_TYPES and other.type.name in _ORDERABLE_TYPES

    def _require_boolean(self, call: str) -> None:
        if not isinstance(self.type, BooleanType):
            raise TypeError(f"{call}(...) requires a Boolean Structure expression")

    def _arithmetic(self, kind: str, other: object, *, reverse: bool = False) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        other_expression = literal(other)
        type = self._arithmetic_type(kind, other_expression)
        arguments = (other_expression, self) if reverse else (self, other_expression)
        return Expression(kind=kind, type=type, nullable=self.nullable or other_expression.nullable, args=arguments)

    def _bitwise_binary(self, kind: str, other: object) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        other_expression = literal(other)
        self._require_integral(other_expression)
        result = (
            LongType()
            if isinstance(self.type, LongType) or isinstance(other_expression.type, LongType)
            else IntegerType()
        )
        return Expression(
            kind=kind,
            type=result,
            nullable=self.nullable or other_expression.nullable,
            args=(self, other_expression),
        )

    def _require_integral(self, other: "Expression" | None = None) -> None:
        operands = (self,) if other is None else (self, other)
        if not all(isinstance(operand.type, (IntegerType, LongType)) for operand in operands):
            raise TypeError("Bitwise operations require integral Structure expressions")

    def _arithmetic_type(self, kind: str, other: "Expression") -> StructureType:
        types = (self.type, other.type)
        if not all(isinstance(type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)) for type in types):
            raise TypeError("Arithmetic requires numeric Structure expressions")
        numeric = cast(tuple[StructureType, StructureType], types)
        if any(isinstance(type, DoubleType) for type in numeric):
            return DoubleType()
        if any(isinstance(type, DecimalType) for type in numeric) and any(
            isinstance(type, FloatType) for type in numeric
        ):
            return DoubleType()
        if any(isinstance(type, FloatType) for type in numeric):
            return FloatType()
        if any(isinstance(type, DecimalType) for type in numeric):
            return self._decimal_arithmetic_type(kind, numeric)
        if kind == "div":
            return DoubleType()
        if any(isinstance(type, LongType) for type in numeric):
            return LongType()
        return IntegerType()

    def _decimal_arithmetic_type(self, kind: str, operands: tuple[StructureType, StructureType]) -> DecimalType:
        left, right = (self._decimal_operand(type) for type in operands)
        if kind in {"add", "sub"}:
            scale = max(left.scale, right.scale)
            precision = max(left.precision - left.scale, right.precision - right.scale) + scale + 1
        elif kind == "mul":
            scale = left.scale + right.scale
            precision = left.precision + right.precision + 1
        elif kind == "div":
            scale = max(6, left.scale + right.precision + 1)
            precision = left.precision - left.scale + right.scale + scale
        elif kind == "mod":
            scale = max(left.scale, right.scale)
            precision = min(left.precision - left.scale, right.precision - right.scale) + scale
        else:
            return left
        return self._bounded_decimal_type(precision, scale)

    def _decimal_operand(self, type: StructureType) -> DecimalType:
        if isinstance(type, DecimalType):
            return type
        if isinstance(type, LongType):
            return DecimalType(precision=20, scale=0)
        return DecimalType(precision=10, scale=0)

    def _bounded_decimal_type(self, precision: int, scale: int) -> DecimalType:
        if precision <= 38:
            return DecimalType(precision=precision, scale=scale)
        if scale < 6:
            return DecimalType(precision=38, scale=scale)
        integer_digits = precision - scale
        if integer_digits > 32:
            return DecimalType(precision=38, scale=6)
        return DecimalType(precision=38, scale=min(38 - integer_digits, scale))

    def _string_predicate(self, name: str, pattern: str) -> "Expression":
        if not isinstance(self.type, StringType):
            raise TypeError(f"{name}(...) requires a String Structure expression")
        if not isinstance(pattern, str):
            raise TypeError(f"{name}(...) requires a string literal")
        return Expression(
            kind=name, type=BooleanType(), nullable=self.nullable, data={"pattern": pattern}, args=(self,)
        )

    def _cast(self, target: StructureType) -> "Expression":
        if not isinstance(target, StructureType) or target.name in {"array", "map", "struct"}:
            raise TypeError("cast(...) requires a scalar Structure type")
        return Expression(
            kind="cast",
            type=target,
            nullable=self.nullable,
            data={"spark_type": self._spark_type(target)},
            args=(self,),
        )

    def _order(self, direction: str) -> "Expression":
        return Expression(
            kind="order", type=self.type, nullable=self.nullable, data={"direction": direction}, args=(self,)
        )

    def _spark_type(self, target: StructureType) -> str:
        if target.name == "integer":
            return "int"
        if target.name == "long":
            return "bigint"
        if target.name == "decimal":
            precision = getattr(target, "precision")
            scale = getattr(target, "scale")
            return f"decimal({precision},{scale})"
        return target.name
