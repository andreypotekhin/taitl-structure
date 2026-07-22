from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass

from structure.core.compiler.frontend.logic.CompilerTransformMember import CompilerTransformMember


@dataclass(frozen=True)
class ParentStepInvocation:
    value: object
    source: Mapping[str, object]


class PatchParentStepCalls:
    """Temporarily expose only overridden steps as legal parent calls."""

    @contextmanager
    def __call__(
        self,
        active: CompilerTransformMember,
        *,
        invoke: Callable[[CompilerTransformMember], ParentStepInvocation],
        record_source: Callable[[Mapping[str, object]], None],
    ) -> Iterator[None]:
        originals: list[tuple[type, str, object]] = []
        scheduled: dict[CompilerTransformMember, ParentStepInvocation] = {}

        def stub(candidate: CompilerTransformMember):
            def call(_self, *args, **kwargs):
                if kwargs:
                    raise TypeError("Parent step method calls must use positional schema arguments")
                invocation = scheduled.get(candidate)
                if invocation is None:
                    invocation = invoke(candidate)
                    scheduled[candidate] = invocation
                record_source(invocation.source)
                return invocation.value

            return call

        try:
            for candidate in active.overridden:
                originals.append((candidate.owner, candidate.name, candidate.owner.__dict__[candidate.name]))
                setattr(candidate.owner, candidate.name, stub(candidate))
            yield
        finally:
            for owner, name, original in reversed(originals):
                setattr(owner, name, original)
