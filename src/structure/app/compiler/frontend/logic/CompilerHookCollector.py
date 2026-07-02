from __future__ import annotations

from typing import TYPE_CHECKING

from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.dsl.model.transforms.Transform import Transform

if TYPE_CHECKING:
    from structure.app.compiler.frontend.logic.CompilerTransformMember import CompilerTransformMember


class CompilerHookCollector:

    def collect(
        self,
        transform_class: type[Transform],
        members: tuple[CompilerTransformMember, ...],
    ) -> dict[tuple[str, tuple[type[Transform], str, int]], tuple[HookPlan, ...]]:
        grouped: dict[tuple[str, tuple[type[Transform], str, int]], list[HookPlan]] = {}
        targets = self._targets(members)
        for item in members:
            name = item.name
            member = item.member
            metadata = getattr(member, "_structure_hook", None)
            if metadata is None:
                continue

            target = targets.get(metadata.get("target_object")) or self._target_by_name(targets, metadata["target"])
            if target is None:
                continue
            key = (metadata["phase"], target.key)
            grouped.setdefault(key, []).append(
                HookPlan(
                    name=name,
                    phase=metadata["phase"],
                    target=metadata["target"],
                    lanes=metadata["lanes"],
                    outputs=metadata["outputs"],
                    pass_inputs=metadata["pass_inputs"],
                    schema_mode=metadata["schema_mode"],
                    project_output=metadata["project_output"],
                    streaming_safe=metadata["streaming_safe"],
                    target_backend=metadata["target_backend"],
                    target_defaulted=metadata["target_defaulted"],
                )
            )
        return {key: tuple(value) for key, value in grouped.items()}

    def _targets(
        self,
        members: tuple[CompilerTransformMember, ...],
    ) -> dict[object, CompilerTransformMember]:
        targets: dict[object, CompilerTransformMember] = {}
        for item in members:
            targets[item.member] = item
            for overridden in item.overridden:
                targets[overridden.member] = overridden
        return targets

    def _target_by_name(
        self,
        targets: dict[object, CompilerTransformMember],
        name: str,
    ) -> CompilerTransformMember | None:
        matches = [target for target in targets.values() if target.name == name]
        if len(matches) == 1:
            return matches[0]
        return None
