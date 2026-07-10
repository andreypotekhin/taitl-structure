from __future__ import annotations

import hashlib
import inspect
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from structure.app.compiler.api import Compiler
from structure.app.compiler.artifacts.commands.BuildArtifactFingerprint import BuildArtifactFingerprint
from structure.app.compiler.artifacts.commands.BuildArtifactManifest import BuildArtifactManifest
from structure.app.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.app.compiler.artifacts.model.CompileKey import CompileKey
from structure.app.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.app.runtime.schemas.api import Schemas
from structure.app.target.capabilities.api import Capabilities
from structure.app.target.pyspark.api import PySpark


class BuildCompiledTransform:

    def __init__(self) -> None:
        self._manifest = BuildArtifactManifest()
        self._fingerprint = BuildArtifactFingerprint()

    def __call__(
        self,
        subject: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        schema_types=None,
        materialize_schemas: bool = True,
    ) -> CompiledTransform:
        capabilities = Capabilities.resolve()(
            target_backend=options.target_backend,
            target_profile=options.target_profile,
            target_variant=options.target_variant,
        )
        manifest = self._manifest(subject, options=options, capability=self._capability(options))
        transform_plan = Compiler.frontend.compile()(subject, warn_on_udfs=options.warn_on_udfs)
        pyspark_plan = PySpark.plan.lower()(transform_plan, capabilities=capabilities)
        schemas = Schemas.build()(pyspark_plan, types=schema_types) if materialize_schemas else None
        artifact = CompiledTransform(
            key=self.key(subject, options=options, manifest=manifest.fingerprint),
            transform_plan=transform_plan,
            pyspark_plan=pyspark_plan,
            schemas=schemas,
            semantic_fingerprint="",
        )
        return CompiledTransform(
            key=artifact.key,
            transform_plan=artifact.transform_plan,
            pyspark_plan=artifact.pyspark_plan,
            schemas=artifact.schemas,
            semantic_fingerprint=self._fingerprint(artifact),
        )

    def key(
        self,
        subject: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        manifest: str | None = None,
    ) -> CompileKey:
        classes = self._classes(subject)
        return CompileKey(
            subject=tuple(f"{cls.__module__}.{cls.__qualname__}" for cls in classes),
            structure_version=self._structure_version(),
            options=options.fingerprint(),
            sources=tuple(self._source(cls) for cls in classes),
            manifest=manifest or self._manifest(subject, options=options, capability=self._capability(options)).fingerprint,
        )

    def _capability(self, options: CompilerOptions) -> str:
        return f"{options.target_backend}:{options.target_profile}:{options.target_variant}"

    def _classes(self, subject: type[Transform] | TransformPipeline) -> tuple[type[Transform], ...]:
        if isinstance(subject, TransformPipeline):
            return tuple(stage.transform_class for stage in subject.stages)
        return (subject,)

    def _source(self, transform: type[Transform]) -> tuple[str, int | None, int | None, str | None]:
        source = inspect.getsourcefile(transform)
        if source is None:
            return (f"{transform.__module__}.{transform.__qualname__}", None, None, None)
        path = Path(source)
        try:
            stat = path.stat()
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            return (path.as_posix(), None, None, None)
        return (path.as_posix(), stat.st_mtime_ns, stat.st_size, digest)

    def _structure_version(self) -> str:
        try:
            return version("structure")
        except PackageNotFoundError:
            return "unknown"


build_compiled_transform = BuildCompiledTransform()
