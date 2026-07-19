from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Mapping, cast

from structure.core.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.platforms.api.Platform import Platform
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.platform.api.v1 import CompileRequest, PlatformCompilation, SchemaValidationRequest


class CompilePlatformTransform:
    def __init__(self, registry=None) -> None:
        self._analyze = CompileTransform()
        self._registry = registry

    def __call__(
        self,
        transform: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions | None = None,
        config: StructureConfig | None = None,
        project_root: Path | str | None = None,
        overrides: Mapping[str, object] | None = None,
        schema_types=None,
        materialize_schemas: bool = True,
        registry=None,
        **settings: object,
    ) -> PlatformCompilation:
        if options is not None:
            if config is not None or project_root is not None or overrides or settings:
                raise ValueError("Pass either options=CompilerOptions, or config/project_root overrides, not both.")
            return self._compile_options(
                transform,
                options=options,
                schema_types=schema_types,
                materialize_schemas=materialize_schemas,
                registry=registry,
            )
        resolved = self._config(config=config, project_root=project_root, overrides=overrides, settings=settings)
        target = self._target(transform, default=resolved.target_backend)
        configuration = {
            "profile": resolved.target_profile,
            "variant": resolved.target_variant,
            "warn_on_udfs": resolved.warn_on_udfs,
            "generated_code_options": resolved.generated_code_options,
            "schema_types": schema_types,
            "materialize_schemas": materialize_schemas,
        }
        platform = (registry or self._registry or Platform.registry()).select(target)
        self._validate_declared_schemas(transform, platform, configuration)
        plan = self._analyze(
            transform,
            config=resolved,
            _authoring=platform.api.authoring,
            _authoring_target=target,
            _authoring_configuration=configuration,
        )
        return self._compile(
            transform,
            plan,
            target=target,
            configuration=configuration,
            registry=registry,
            platform=platform,
        )

    def _compile_options(
        self,
        transform: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        schema_types,
        materialize_schemas: bool,
        registry,
    ) -> PlatformCompilation:
        target = self._target(transform, default=options.target_backend)
        configuration = {
            "profile": options.target_profile,
            "variant": options.target_variant,
            "warn_on_udfs": options.warn_on_udfs,
            "generated_code_options": options.generated_code_options,
            "schema_types": schema_types,
            "materialize_schemas": materialize_schemas,
        }
        platform = (registry or self._registry or Platform.registry()).select(target)
        self._validate_declared_schemas(transform, platform, configuration)
        plan = self._analyze(
            transform,
            project_root=options.project_root,
            warn_on_udfs=options.warn_on_udfs,
            generated_code_options=options.generated_code_options,
            _authoring=platform.api.authoring,
            _authoring_target=target,
            _authoring_configuration=configuration,
        )
        return self._compile(
            transform,
            plan,
            target=target,
            configuration=configuration,
            registry=registry,
            platform=platform,
        )

    def _compile(
        self,
        transform: type[Transform] | TransformPipeline,
        plan,
        *,
        target: str,
        configuration: Mapping[str, object],
        registry,
        platform=None,
    ) -> PlatformCompilation:
        platform = platform or (registry or self._registry or Platform.registry()).select(target)
        compilation = platform.api.compiler.compile(
            CompileRequest(transform=transform, target=target, configuration=configuration, analysis=plan)
        )
        if not isinstance(compilation, PlatformCompilation):
            raise ValueError(f"PLATFORM-E2708: Platform {target!r} returned an invalid compilation result.")
        return replace(compilation, analysis=plan)

    @staticmethod
    def _validate_declared_schemas(
        transform: type[Transform] | TransformPipeline, platform, configuration: Mapping[str, object]
    ) -> None:
        validate = getattr(platform.api.schema, "validate", None)
        if not callable(validate):
            return
        schemas: dict[object, None] = {}
        for transform_class in CompilePlatformTransform._transform_classes(transform):
            declarations = (
                *transform_class._structure_inputs.values(),
                *transform_class._structure_lanes.values(),
                *transform_class._structure_outputs.values(),
            )
            for declaration in declarations:
                schemas[cast(object, getattr(declaration, "schema"))] = None
        validate(SchemaValidationRequest(schemas=tuple(schemas), configuration=configuration))

    @staticmethod
    def _transform_classes(transform: type[Transform] | TransformPipeline) -> tuple[type[Transform], ...]:
        pipeline = transform if isinstance(transform, TransformPipeline) else transform._structure_pipeline
        if pipeline is None:
            return (transform,)  # type: ignore[return-value]
        return tuple(stage.transform_class for stage in pipeline.stages)

    def _config(
        self,
        *,
        config: StructureConfig | None,
        project_root: Path | str | None,
        overrides: Mapping[str, object] | None,
        settings: Mapping[str, object],
    ) -> StructureConfig:
        if config is not None and (project_root is not None or overrides or settings):
            raise ValueError(
                "Pass either config=StructureConfig.resolve(...), or pass project_root/config override fields, not both."
            )
        merged = dict(overrides or {})
        duplicates = set(merged).intersection(settings)
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise ValueError(f"Configuration override supplied twice: {names}.")
        merged.update(settings)
        return config or StructureConfig.resolve(project_root=project_root, overrides=merged)

    def _target(self, transform: type[Transform] | TransformPipeline, *, default: str) -> str:
        configuration = PlatformConfiguration.resolve({"platform": {"default": default}})
        return Platform.resolve_target()(transform, configuration=configuration)
