from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, cast

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
        if any(isinstance(type, LongType) for type in numeric):
            return LongType()
        return IntegerType()

    def _decimal_arithmetic_type(
        self, kind: str, operands: tuple[StructureType, StructureType]
    ) -> DecimalType:
        left, right = (self._decimal_operand(type) for type in operands)
        if kind in {"add", "sub"}:
            scale = max(left.scale, right.scale)
            precision = max(left.precision - left.scale, right.precision - right.scale) + scale + 1
        else:
            scale = left.scale + right.scale
            precision = left.precision + right.precision + 1
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
