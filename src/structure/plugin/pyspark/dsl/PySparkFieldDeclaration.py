"""PySpark field tokens usable in schema CHECK predicates."""

from structure.dsl import FieldDeclaration
from structure.plugin.pyspark.dsl.Expression import Expression


class PySparkFieldDeclaration(FieldDeclaration):
    __hash__ = object.__hash__

    def _structure_expression(self):
        return Expression(kind="field_declaration", type=self.type, nullable=self.nullable, data={"declaration": self})

    def __getattr__(self, name: str):
        return getattr(self._structure_expression(), name)

    def __eq__(self, other: object):  # type: ignore[override]
        return self._structure_expression() == other

    def __ne__(self, other: object):  # type: ignore[override]
        return self._structure_expression() != other

    def __gt__(self, other: object):
        return self._structure_expression() > other

    def __ge__(self, other: object):
        return self._structure_expression() >= other

    def __lt__(self, other: object):
        return self._structure_expression() < other

    def __le__(self, other: object):
        return self._structure_expression() <= other

    def __and__(self, other: object):
        return self._structure_expression() & other

    def __or__(self, other: object):
        return self._structure_expression() | other

    def __invert__(self):
        return ~self._structure_expression()

    def __add__(self, other: object):
        return self._structure_expression() + other

    def __sub__(self, other: object):
        return self._structure_expression() - other

    def __mul__(self, other: object):
        return self._structure_expression() * other

    def __truediv__(self, other: object):
        return self._structure_expression() / other

    def __bool__(self) -> bool:
        raise TypeError("Structure field declarations cannot be used as Python booleans. Use &, |, or ~.")
