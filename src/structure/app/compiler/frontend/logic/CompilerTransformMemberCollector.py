from __future__ import annotations

import inspect
from collections import defaultdict

from structure.app.compiler.diagnostics.api import StructureCompileError
from structure.app.compiler.frontend.logic.CompilerTransformMember import CompilerTransformMember
from structure.app.dsl.model.transforms.Transform import Transform
from structure.lib.cross.errors import Diagnostic, diagnostic_registry


class CompilerTransformMemberCollector:

    def collect(self, transform_class: type[Transform]) -> tuple[CompilerTransformMember, ...]:
        candidates = self._candidates(transform_class)
        members: list[CompilerTransformMember] = []
        for name, group in self._groups(candidates).items():
            members.append(self._effective(transform_class, name, group))
        return tuple(sorted(members, key=lambda member: member.position))

    def _candidates(self, transform_class: type[Transform]) -> list[CompilerTransformMember]:
        classes = self._classes(transform_class)
        candidates: list[CompilerTransformMember] = []
        for cls in classes:
            for name, member in cls.__dict__.items():
                if name.startswith("_") or name == "run" or not inspect.isfunction(member):
                    continue
                candidates.append(
                    CompilerTransformMember(
                        owner=cls,
                        name=name,
                        member=member,
                        position=len(candidates),
                    )
                )
        return candidates

    def _classes(self, transform_class: type[Transform]) -> tuple[type[Transform], ...]:
        classes: list[type[Transform]] = []
        visited: set[type[Transform]] = set()

        def visit(cls: type[Transform]) -> None:
            if cls in visited or cls is Transform:
                return
            for base in cls.__bases__:
                if isinstance(base, type) and issubclass(base, Transform):
                    visit(base)
            visited.add(cls)
            classes.append(cls)

        visit(transform_class)
        return tuple(classes)

    def _groups(
        self,
        candidates: list[CompilerTransformMember],
    ) -> dict[str, list[CompilerTransformMember]]:
        groups: dict[str, list[CompilerTransformMember]] = defaultdict(list)
        for candidate in candidates:
            groups[candidate.name].append(candidate)
        return dict(groups)

    def _effective(
        self,
        transform_class: type[Transform],
        name: str,
        candidates: list[CompilerTransformMember],
    ) -> CompilerTransformMember:
        active: list[CompilerTransformMember] = []
        overridden: list[CompilerTransformMember] = []
        position = candidates[0].position
        for candidate in candidates:
            replaced = [item for item in active if issubclass(candidate.owner, item.owner)]
            if replaced:
                active = [item for item in active if item not in replaced]
                overridden.extend(replaced)
                position = min(position, *(item.position for item in replaced))
            active.append(candidate)
        if len(active) > 1:
            owners = ", ".join(f"{item.owner.__name__}.{name}" for item in active)
            raise StructureCompileError(
                Diagnostic(
                    entry=diagnostic_registry.get("DSL-E0402"),
                    problem=f"{transform_class.__name__} inherits ambiguous transform member {name}: {owners}.",
                    use=f"Override {name} on {transform_class.__name__} or rename one parent method.",
                    context={"member": name},
                    source=f"{transform_class.__module__}.{transform_class.__name__}.{name}",
                )
            )
        member = active[0]
        return CompilerTransformMember(
            owner=member.owner,
            name=member.name,
            member=member.member,
            position=position,
            overridden=tuple(overridden),
        )
