from __future__ import annotations

import inspect
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from structure.core.compiler.frontend.logic.CompilerTransformMember import CompilerTransformMember
from structure.core.dsl.model.transforms.Transform import Transform


class GuardTransformStepCalls:
    """Enforce Core's source-order rule while a transform step is invoked."""

    def __init__(self, *, error: Callable[..., Exception], is_step: Callable[[object], bool]) -> None:
        self._error = error
        self._is_step = is_step

    @contextmanager
    def __call__(
        self,
        transform_class: type[Transform],
        members: tuple[CompilerTransformMember, ...],
        *,
        active: CompilerTransformMember,
    ) -> Iterator[None]:
        originals: list[tuple[type[Transform], str, object]] = []
        guarded: set[tuple[type[Transform], str]] = set()

        def guard(owner: type[Transform], name: str):
            def call(_self, *args, **kwargs):
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=active.name,
                    problem=(
                        f"{transform_class.__name__}.{active.name} calls compiled step method "
                        f"{owner.__name__}.{name} directly."
                    ),
                    use=(
                        "Step methods are pipeline steps. Use source order, lane bindings, Transform.to(...), "
                        "a private helper, or @special(type=\"expr\") instead. Only an override may call its overridden "
                        "parent step method."
                    ),
                    context={"called_step_method": f"{owner.__name__}.{name}"},
                )

            return call

        try:
            for candidate in self._members(transform_class, members):
                key = (candidate.owner, candidate.name)
                if key in guarded:
                    continue
                originals.append((candidate.owner, candidate.name, candidate.owner.__dict__[candidate.name]))
                setattr(candidate.owner, candidate.name, guard(candidate.owner, candidate.name))
                guarded.add(key)
            yield
        finally:
            for owner, name, original in reversed(originals):
                setattr(owner, name, original)

    def _members(
        self,
        transform_class: type[Transform],
        members: tuple[CompilerTransformMember, ...],
    ) -> tuple[CompilerTransformMember, ...]:
        guarded: list[CompilerTransformMember] = []
        seen: set[tuple[type[Transform], str]] = set()

        def add(candidate: CompilerTransformMember) -> None:
            key = (candidate.owner, candidate.name)
            if key not in seen and self._is_step(candidate.member):
                guarded.append(candidate)
                seen.add(key)

        for member in members:
            add(member)
            for candidate in member.overridden:
                add(candidate)
        for cls in (*transform_class.__mro__, *self._loaded_classes()):
            if cls is Transform or not isinstance(cls, type) or not issubclass(cls, Transform):
                continue
            for name, member in cls.__dict__.items():
                if not name.startswith("_") and name != "run" and inspect.isfunction(member):
                    add(CompilerTransformMember(owner=cls, name=name, member=member, position=0))
        return tuple(guarded)

    @staticmethod
    def _loaded_classes() -> tuple[type[Transform], ...]:
        classes: list[type[Transform]] = []

        def visit(cls: type[Transform]) -> None:
            for subclass in cls.__subclasses__():
                classes.append(subclass)
                visit(subclass)

        visit(Transform)
        return tuple(classes)
