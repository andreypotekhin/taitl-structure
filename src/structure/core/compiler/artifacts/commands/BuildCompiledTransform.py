from __future__ import annotations

import hashlib
import inspect
from pathlib import Path

from structure.core.compiler.artifacts.commands.BuildArtifactFingerprint import BuildArtifactFingerprint
from structure.core.compiler.artifacts.commands.BuildArtifactManifest import BuildArtifactManifest
from structure.core.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.core.compiler.artifacts.model.CompileKey import CompileKey
from structure.core.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.platforms.api.Platform import Platform
from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.platform.api.v1.CompileRequest import CompileRequest
from structure.version import VERSION


class BuildCompiledTransform:

    def __init__(self, registry=None) -> None:
        self._manifest = BuildArtifactManifest()
        self._fingerprint = BuildArtifactFingerprint()
        self._registry = registry or Platform.registry()

    def __call__(
        self,
        subject: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        schema_types=None,
        materialize_schemas: bool = True,
    ) -> CompiledTransform:
        manifest = self._manifest(subject, options=options, capability=self._capability(options))
        platform = self._registry.select(options.target_backend)
        compilation = platform.api.compiler.compile(
            CompileRequest(
                transform=subject,
                target=options.target_backend,
                configuration={
                    "profile": options.target_profile,
                    "variant": options.target_variant,
                    "warn_on_udfs": options.warn_on_udfs,
                    "generated_code_options": options.generated_code_options,
                    "schema_types": schema_types,
                    "materialize_schemas": materialize_schemas,
                },
            )
        )
        if not isinstance(compilation.analysis, TransformPlan):
            raise ValueError(f"PLATFORM-E2708: Platform {options.target_backend!r} returned an invalid compilation.")
        schemas = self._schemas(compilation.schemas, materialize=materialize_schemas)
        artifact = CompiledTransform(
            key=self.key(subject, options=options, manifest=manifest.fingerprint),
            transform_plan=compilation.analysis,
            payload=compilation.lowered,
            schemas=schemas,
            semantic_fingerprint="",
        )
        return CompiledTransform(
            key=artifact.key,
            transform_plan=artifact.transform_plan,
            payload=artifact.payload,
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
            manifest=manifest
            or self._manifest(subject, options=options, capability=self._capability(options)).fingerprint,
        )

    def _capability(self, options: CompilerOptions) -> str:
        return f"{options.target_backend}:{options.target_profile}:{options.target_variant}"

    def _schemas(self, value: object | None, *, materialize: bool) -> TransformSchemas | None:
        if not materialize:
            return None
        if not isinstance(value, TransformSchemas):
            raise ValueError("PLATFORM-E2708: Platform compilation did not provide transform schemas.")
        return value

    def _classes(self, subject: type[Transform] | TransformPipeline) -> tuple[type[Transform], ...]:
        if isinstance(subject, TransformPipeline):
            return tuple(stage.transform_class for stage in subject.stages)
        return (subject,)

    def _source(self, transform: type[Transform]) -> tuple[str, int | None, int | None, str | None]:
        from structure.core.sources.model.StructureSources import source_origin

        origin = source_origin(transform)
        if origin is not None:
            return (origin.path, None, None, origin.digest)
        try:
            source = inspect.getsourcefile(transform)
        except TypeError:
            source = None
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
        return VERSION


build_compiled_transform = BuildCompiledTransform()
