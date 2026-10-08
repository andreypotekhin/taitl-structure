from __future__ import annotations

from collections.abc import Callable

from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.core.dsl.model.transforms.Transform import Transform

Declaration = InputDeclaration | OutputDeclaration
ErrorFactory = Callable[..., StructureCompileError]


class ValidateInheritedTransformDeclarations:
    """Reject inherited declarations that silently change table storage or role."""

    def __init__(self, error: ErrorFactory) -> None:
        self._error = error

    def __call__(self, transform_class: type[Transform]) -> None:
        ancestors: dict[str, list[tuple[type[Transform], Declaration]]] = {}
        for ancestor in transform_class.__mro__[1:]:
            if not isinstance(ancestor, type) or not issubclass(ancestor, Transform):
                continue
            for name, value in ancestor.__dict__.items():
                if isinstance(value, (InputDeclaration, OutputDeclaration)):
                    ancestors.setdefault(name, []).append((ancestor, value))

        for name, inherited in ancestors.items():
            current = transform_class.__dict__.get(name)
            current_role = (
                self._table_role(current) if isinstance(current, (InputDeclaration, OutputDeclaration)) else None
            )
            inherited_tables = [
                (owner, declaration, role)
                for owner, declaration in inherited
                if (role := self._table_role(declaration)) is not None
            ]
            if not inherited_tables and current_role is None:
                continue

            roles = {(role, declaration.schema) for _, declaration, role in inherited_tables}
            if len(roles) > 1:
                self._raise(
                    transform_class,
                    name,
                    inherited_tables[0][0],
                    "Inherited table declarations disagree on provider, role, or schema.",
                    "Resolve the table declaration explicitly in the child or split the parent transforms.",
                )

            if current is None:
                continue

            owner, declaration = inherited[0]
            role = self._table_role(declaration)
            same_schema = (
                current.schema is declaration.schema
                if isinstance(current, (InputDeclaration, OutputDeclaration))
                else False
            )
            if current_role != role or not same_schema:
                self._raise(
                    transform_class,
                    name,
                    owner,
                    f"{transform_class.__name__}.{name} changes an inherited table binding from {role} "
                    f"with schema {declaration.schema.__name__}.",
                    "Keep the inherited provider, role, and schema. Use a separate declaration or compose an explicit adapter.",
                )

    @staticmethod
    def _table_role(declaration: Declaration) -> str | None:
        if isinstance(declaration, InputDeclaration):
            if declaration.binding == "dataframe":
                return None
            return f"{declaration.binding}_input"
        if declaration.binding == "dataframe":
            return None
        if declaration.binding == "delta_table":
            return "delta_table"
        if declaration.binding == "iceberg_table":
            return "iceberg_table"
        if declaration.binding in {"delta", "iceberg"}:
            return f"{declaration.binding}_output"
        return None

    def _raise(
        self,
        transform_class: type[Transform],
        name: str,
        ancestor: type[Transform],
        problem: str,
        use: str,
    ) -> None:
        raise self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=name,
            problem=f"{problem} Ancestor: {ancestor.__name__}.",
            use=use,
            context={"member": name, "ancestor": ancestor.__name__},
        )
