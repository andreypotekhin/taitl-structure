from __future__ import annotations

import os
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from structure.core.compiler.artifacts.model import CompiledArtifactPool, CompilerOptions

_REUSE_ENV = "STRUCTURE_COMPILED_ARTIFACT_REUSE"
_PROFILE_ENV = "STRUCTURE_PROFILE_COMPILATION"
_MODES = {"module", "off"}


@dataclass(frozen=True)
class CompilationRecord:
    transform: str
    phase: str
    elapsed: float
    hits: int
    misses: int
    loaded: int
    outcome: str


def reuse_mode() -> str:
    value = os.environ.get(_REUSE_ENV, "module").lower()
    if value not in _MODES:
        allowed = ", ".join(sorted(_MODES))
        raise ValueError(f"{_REUSE_ENV} must be one of {allowed}; got {value!r}.")
    return value


def _identity(subject: Any) -> str:
    transform = subject if isinstance(subject, type) else type(subject)
    return f"{transform.__module__}.{transform.__qualname__}"


class ProfiledCompiledArtifactPool(CompiledArtifactPool):
    """Integration-only pool that measures complete compiler-cache requests."""

    def __init__(self, owner: "CompiledArtifacts", phase: str) -> None:
        super().__init__()
        self.owner = owner
        self.phase = phase

    def get_or_compile(self, subject, *, options: CompilerOptions, schema_types=None, force: bool = False):
        before = self.status()
        started = perf_counter()
        outcome = "success"
        try:
            return super().get_or_compile(
                subject,
                options=options,
                schema_types=schema_types,
                force=force,
            )
        except Exception:
            outcome = "failure"
            raise
        finally:
            after = self.status()
            elapsed = perf_counter() - started
            hit_delta = after.hits - before.hits
            miss_delta = after.misses - before.misses
            record = CompilationRecord(
                transform=_identity(subject),
                phase=self.phase,
                elapsed=elapsed,
                hits=hit_delta,
                misses=miss_delta,
                loaded=after.loaded - before.loaded,
                outcome=outcome,
            )
            self.owner.records.append(record)
            if os.environ.get(_PROFILE_ENV) == "1":
                print(
                    f"[compile] {record.phase} {record.transform}: {record.elapsed:.2f}s "
                    f"hits={record.hits} misses={record.misses} outcome={record.outcome}",
                    flush=True,
                )


class CompiledArtifacts:
    """Own compiler pools for one integration-test module."""

    def __init__(self, mode: str | None = None) -> None:
        self.mode = mode or reuse_mode()
        if self.mode not in _MODES:
            raise ValueError(f"Unsupported compiled-artifact reuse mode: {self.mode!r}.")
        self.records: list[CompilationRecord] = []
        self._pools: list[ProfiledCompiledArtifactPool] = []
        self._module_pool = self._new_pool("module", retain=True) if self.mode == "module" else None

    def pool(self, phase: str) -> CompiledArtifactPool:
        if self.mode == "module":
            pool = self._module_pool
            assert pool is not None
            pool.phase = phase
            return pool
        return self._new_pool(phase, retain=False)

    def _new_pool(self, phase: str, *, retain: bool) -> ProfiledCompiledArtifactPool:
        pool = ProfiledCompiledArtifactPool(self, phase)
        if retain:
            self._pools.append(pool)
        return pool

    def totals(self) -> dict[str, float | int]:
        return {
            "requests": len(self.records),
            "hits": sum(record.hits for record in self.records),
            "misses": sum(record.misses for record in self.records),
            "elapsed": sum(record.elapsed for record in self.records),
        }

    def close(self) -> None:
        if os.environ.get(_PROFILE_ENV) == "1" and self.records:
            totals = self.totals()
            print(
                f"[compile] module total: {totals['elapsed']:.2f}s requests={totals['requests']} "
                f"hits={totals['hits']} misses={totals['misses']}",
                flush=True,
            )
        for pool in self._pools:
            pool.clear()
        self._pools.clear()
        self._module_pool = None
