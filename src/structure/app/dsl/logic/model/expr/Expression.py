from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from structure.app.dsl.logic.model.types.StructType import StructType
from structure.app.dsl.logic.model.types.StructureType import StructureType


@dataclass(frozen=True, eq=False)
class Expression:
    kind: str
    type: StructureType | None = None
    nullable: bool = True
    data: Mapping[str, object] | None = None
    args: tuple["Expression", ...] = ()

    def is_null(self) -> "Expression":
        return Expression(kind="is_null", type=None, nullable=False, args=(self,))

    def is_not_null(self) -> "Expression":
        return Expression(kind="is_not_null", type=None, nullable=False, args=(self,))

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
        path = tuple(data.get("path", (data.get("field"),)))
        path = tuple(item for item in path if item)
        path = (*path, name)
        data["field"] = ".".join(path)
        data["path"] = path
        return Expression(kind="field", type=field.type, nullable=field.nullable, data=data)

    def __and__(self, other: object) -> "Expression":
        return self._binary("and", other)

    def __or__(self, other: object) -> "Expression":
        return self._binary("or", other)

    def __invert__(self) -> "Expression":
        return Expression(kind="not", type=None, nullable=False, args=(self,))

    def __eq__(self, other: object) -> "Expression":  # type: ignore[override]
        return self._binary("eq", other)

    def __ne__(self, other: object) -> "Expression":  # type: ignore[override]
        return self._binary("ne", other)

    def __sub__(self, other: object) -> "Expression":
        return self._binary("sub", other, type=self.type, nullable=self.nullable)

    def __gt__(self, other: object) -> "Expression":
        return self._binary("gt", other)

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
        from structure.app.dsl.logic.model.expr.expressions import literal

        return Expression(kind=kind, type=type, nullable=nullable, args=(self, literal(other)))
