from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, cast

from structure.app.dsl.model.types.BooleanType import BooleanType
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
