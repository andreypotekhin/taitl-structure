from __future__ import annotations

import hashlib
import inspect
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from structure.app.compiler.artifacts.model.ArtifactDependency import ArtifactDependency
from structure.app.compiler.artifacts.model.ArtifactManifest import ArtifactManifest
from structure.app.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline


class BuildArtifactManifest:

    def __call__(
        self,
        subject: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        capability: str,
    ) -> ArtifactManifest:
        dependencies = tuple(
            sorted(
                self._dependencies(subject, project_root=options.project_root),
                key=lambda dependency: (dependency.kind, dependency.name),
            )
        )
        return ArtifactManifest(
            dependencies=dependencies,
            options=options.fingerprint(),
            structure_version=self._structure_version(),
            capability=capability,
        )

    def _dependencies(
        self,
        subject: type[Transform] | TransformPipeline,
        *,
        project_root: Path,
    ) -> set[ArtifactDependency]:
        dependencies: set[ArtifactDependency] = set()
        for transform in self._classes(subject):
            for owner in transform.__mro__:
                if not isinstance(owner, type) or not issubclass(owner, Transform) or owner is Transform:
                    continue
                dependencies.add(self._dependency("transform", owner, project_root=project_root))
                dependencies.update(self._schemas(owner, project_root=project_root))
        return dependencies

    def _classes(self, subject: type[Transform] | TransformPipeline) -> tuple[type[Transform], ...]:
        if isinstance(subject, TransformPipeline):
            return tuple(stage.transform_class for stage in subject.stages)
        return (subject,)

    def _schemas(self, transform: type[Transform], *, project_root: Path) -> set[ArtifactDependency]:
        declarations = (
            *transform._structure_inputs.values(),
            *transform._structure_lanes.values(),
            *transform._structure_outputs.values(),
        )
        schemas = (getattr(declaration, "schema", None) for declaration in declarations)
        return {
            self._dependency("schema", schema, project_root=project_root)
            for schema in schemas
            if isinstance(schema, type) and issubclass(schema, Schema)
        }

    def _dependency(self, kind: str, value: type, *, project_root: Path) -> ArtifactDependency:
        name = f"{value.__module__}.{value.__qualname__}"
        source = inspect.getsourcefile(value)
        if source is None:
            return ArtifactDependency(kind=kind, name=name, path=None, digest=None)
        path = Path(source)
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            digest = None
        try:
            display = path.relative_to(project_root).as_posix()
        except ValueError:
            display = path.as_posix()
        return ArtifactDependency(kind=kind, name=name, path=display, digest=digest)

    def _structure_version(self) -> str:
        try:
            return version("structure")
        except PackageNotFoundError:
            return "unknown"
