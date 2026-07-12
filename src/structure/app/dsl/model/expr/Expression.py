from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, cast

from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType


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
        return self._binary("null_safe_eq", other)

    def isin(self, *values: object) -> "Expression":
        from structure.app.dsl.model.expr.expressions import literal

        if not values:
            raise TypeError("isin(...) requires at least one value")
        return Expression(
            kind="isin",
            type=BooleanType(),
            nullable=True,
            args=(self, *(literal(value) for value in values)),
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
        if not isinstance(self.type, StructType):
            raise AttributeError(name)
        fields = self.type.schema._structure_fields
        if name not in fields:
            raise AttributeError(name)

        field = fields[name]
        data = dict(self.data or {})
        path_data = data.get("path")
        path = cast(tuple[object, ...], path_data) if isinstance(path_data, tuple) else (data.get("field"),)
        name_path_data = data.get("name_path")
        name_path = (
            cast(tuple[object, ...], name_path_data) if isinstance(name_path_data, tuple) else (data.get("name"),)
        )
        path_strings = tuple(str(item) for item in path if item)
        name_path_strings = tuple(str(item) for item in name_path if item)
        path_strings = (*path_strings, field.column)
        name_path_strings = (*name_path_strings, name)
        data["field"] = ".".join(path_strings)
        data["field_nullable"] = field.nullable
        data["name"] = ".".join(name_path_strings)
        data["path"] = path_strings
        data["name_path"] = name_path_strings
        return Expression(kind="field", type=field.type, nullable=self.nullable or field.nullable, data=data)

    def __and__(self, other: object) -> "Expression":
        return self._binary("and", other, type=BooleanType())

    def __or__(self, other: object) -> "Expression":
        return self._binary("or", other, type=BooleanType())

    def __invert__(self) -> "Expression":
        return Expression(kind="not", type=BooleanType(), nullable=False, args=(self,))

    def __eq__(self, other: object) -> "Expression":  # type: ignore[override]
        return self._binary("eq", other, type=BooleanType())

    def __ne__(self, other: object) -> "Expression":  # type: ignore[override]
        return self._binary("ne", other, type=BooleanType())

    def __add__(self, other: object) -> "Expression":
        return self._binary("add", other, type=self.type, nullable=self.nullable)

    def __radd__(self, other: object) -> "Expression":
        return self._reverse_binary("add", other, type=self.type, nullable=self.nullable)

    def __sub__(self, other: object) -> "Expression":
        return self._binary("sub", other, type=self.type, nullable=self.nullable)

    def __rsub__(self, other: object) -> "Expression":
        return self._reverse_binary("sub", other, type=self.type, nullable=self.nullable)

    def __mul__(self, other: object) -> "Expression":
        return self._binary("mul", other, type=self.type, nullable=self.nullable)

    def __rmul__(self, other: object) -> "Expression":
        return self._reverse_binary("mul", other, type=self.type, nullable=self.nullable)

    def __gt__(self, other: object) -> "Expression":
        return self._binary("gt", other, type=BooleanType())

    def __lt__(self, other: object) -> "Expression":
        return self._binary("lt", other, type=BooleanType())

    def __le__(self, other: object) -> "Expression":
        return self._binary("le", other, type=BooleanType())

    def __ge__(self, other: object) -> "Expression":
        return self._binary("ge", other, type=BooleanType())

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

    def _string_predicate(self, name: str, pattern: str) -> "Expression":
        if not isinstance(self.type, StringType):
            raise TypeError(f"{name}(...) requires a String Structure expression")
        if not isinstance(pattern, str):
            raise TypeError(f"{name}(...) requires a string literal")
        return Expression(kind=name, type=BooleanType(), nullable=self.nullable, data={"pattern": pattern}, args=(self,))

    def _cast(self, target: StructureType) -> "Expression":
        if not isinstance(target, StructureType) or target.name in {"array", "map", "struct"}:
            raise TypeError("cast(...) requires a scalar Structure type")
        return Expression(kind="cast", type=target, nullable=self.nullable, data={"spark_type": self._spark_type(target)}, args=(self,))

    def _order(self, direction: str) -> "Expression":
        return Expression(kind="order", type=self.type, nullable=self.nullable, data={"direction": direction}, args=(self,))

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
