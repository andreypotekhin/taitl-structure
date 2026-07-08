from __future__ import annotations

import inspect
from pathlib import Path

from structure.app.compiler.api import Compiler
from structure.app.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.app.compiler.artifacts.model.CompileKey import CompileKey
from structure.app.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.app.runtime.schemas.api import Schemas
from structure.app.target.capabilities.api import Capabilities
from structure.app.target.pyspark.api import PySpark


class BuildCompiledTransform:

    def __call__(
        self,
        subject: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        schema_types=None,
    ) -> CompiledTransform:
        capabilities = Capabilities.resolve()(
            target_backend=options.target_backend,
            target_profile=options.target_profile,
            target_variant=options.target_variant,
        )
        transform_plan = Compiler.frontend.compile()(subject)
        pyspark_plan = PySpark.plan.lower()(transform_plan, capabilities=capabilities)
        schemas = Schemas.build()(pyspark_plan, types=schema_types)
        return CompiledTransform(
            key=self.key(subject, options=options),
            transform_plan=transform_plan,
            pyspark_plan=pyspark_plan,
            schemas=schemas,
        )

    def key(self, subject: type[Transform] | TransformPipeline, *, options: CompilerOptions) -> CompileKey:
        classes = self._classes(subject)
        return CompileKey(
            subject=tuple(f"{cls.__module__}.{cls.__qualname__}" for cls in classes),
            options=options.fingerprint(),
            sources=tuple(self._source(cls) for cls in classes),
        )

    def _classes(self, subject: type[Transform] | TransformPipeline) -> tuple[type[Transform], ...]:
        if isinstance(subject, TransformPipeline):
            return tuple(stage.transform_class for stage in subject.stages)
        return (subject,)

    def _source(self, transform: type[Transform]) -> tuple[str, int | None, int | None]:
        source = inspect.getsourcefile(transform)
        if source is None:
            return (f"{transform.__module__}.{transform.__qualname__}", None, None)
        path = Path(source)
        try:
            stat = path.stat()
        except OSError:
            return (path.as_posix(), None, None)
        return (path.as_posix(), stat.st_mtime_ns, stat.st_size)


build_compiled_transform = BuildCompiledTransform()
