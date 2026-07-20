from __future__ import annotations

from threading import Event, RLock

from structure.core.compiler.artifacts.api.Artifacts import Artifacts
from structure.core.compiler.artifacts.model.ArtifactCacheReport import ArtifactCacheReport
from structure.core.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.core.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline


class CompiledArtifactPool:

    def __init__(self, *, max_entries: int | None = None) -> None:
        self._artifacts: dict[object, CompiledTransform] = {}
        self._building: dict[object, Event] = {}
        self._lock = RLock()
        self._max_entries = max_entries
        self._hits = 0
        self._misses = 0
        self._loaded = 0

    def get_or_compile(
        self,
        subject: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        schema_types=None,
        force: bool = False,
    ) -> CompiledTransform:
        builder = Artifacts().build()
        key = builder.key(subject, options=options)
        while True:
            with self._lock:
                artifact = self._artifacts.get(key)
                if artifact is not None and not force:
                    self._hits += 1
                    return artifact
                event = self._building.get(key)
                if event is None:
                    event = Event()
                    self._building[key] = event
                    self._misses += 1
                    break
            event.wait()

        try:
            artifact = builder(subject, options=options, schema_types=schema_types)
            with self._lock:
                existing = self._artifacts.get(key)
                if existing is not None and not force:
                    return existing
                if self._max_entries is not None and len(self._artifacts) >= self._max_entries:
                    self._artifacts.pop(next(iter(self._artifacts)))
                self._artifacts[key] = artifact
                return artifact
        finally:
            with self._lock:
                self._building.pop(key).set()

    def load(self, artifact: CompiledTransform) -> None:
        with self._lock:
            self._artifacts[artifact.key] = artifact
            self._loaded += 1

    def load_many(self, artifacts) -> ArtifactCacheReport:
        for artifact in artifacts:
            self.load(artifact)
        return self.status()

    def clear(self) -> None:
        with self._lock:
            self._artifacts.clear()

    def status(self) -> ArtifactCacheReport:
        with self._lock:
            return ArtifactCacheReport(
                entries=len(self._artifacts),
                hits=self._hits,
                misses=self._misses,
                loaded=self._loaded,
            )
