from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Mapping, cast

from structure.core.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.core.compiler.frontend.commands.AnalyzeTransform import AnalyzeTransform
from structure.core.compiler.frontend.commands.AuthorTransform import AuthorTransform
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.plugins.api.Plugin import Plugin
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.plugin.api.v1 import CompilationPurpose, CompileRequest, PluginCompilation, SchemaValidationRequest


class CompilePluginTransform:
    def __init__(self, registry=None) -> None:
        self._analyze = AnalyzeTransform()
        self._author = AuthorTransform()
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
        purpose: CompilationPurpose = CompilationPurpose.RUNTIME,
        registry=None,
        **settings: object,
    ) -> PluginCompilation:
        if options is not None:
            if config is not None or project_root is not None or overrides or settings:
                raise ValueError("Pass either options=CompilerOptions, or config/project_root overrides, not both.")
            return self._compile_options(
                transform,
                options=options,
                schema_types=schema_types,
                materialize_schemas=materialize_schemas,
                purpose=purpose,
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
        plugin = (registry or self._registry or Plugin.registry()).select(target)
        self._validate_declared_schemas(transform, plugin, configuration)
        analysis = self._analyze(
            transform,
            config=resolved,
        )
        plan = self._author(
            transform,
            analysis,
            config=resolved,
            authoring=plugin.api.authoring,
            target=target,
            configuration=configuration,
        )
        return self._compile(
            transform,
            plan,
            target=target,
            configuration=configuration,
            registry=registry,
            plugin=plugin,
            purpose=purpose,
        )

    def _compile_options(
        self,
        transform: type[Transform] | TransformPipeline,
        *,
        options: CompilerOptions,
        schema_types,
        materialize_schemas: bool,
        purpose: CompilationPurpose,
        registry,
    ) -> PluginCompilation:
        target = self._target(transform, default=options.target_backend)
        configuration = {
            "profile": options.target_profile,
            "variant": options.target_variant,
            "warn_on_udfs": options.warn_on_udfs,
            "generated_code_options": options.generated_code_options,
            "schema_types": schema_types,
            "materialize_schemas": materialize_schemas,
        }
        plugin = (registry or self._registry or Plugin.registry()).select(target)
        self._validate_declared_schemas(transform, plugin, configuration)
        authoring_config = StructureConfig.resolve(
            project_root=options.project_root,
            overrides={
                "warn_on_udfs": options.warn_on_udfs,
                "generated_code_options": options.generated_code_options,
            },
        )
        analysis = self._analyze(
            transform,
            config=authoring_config,
        )
        plan = self._author(
            transform,
            analysis,
            config=authoring_config,
            authoring=plugin.api.authoring,
            target=target,
            configuration=configuration,
        )
        return self._compile(
            transform,
            plan,
            target=target,
            configuration=configuration,
            registry=registry,
            plugin=plugin,
            purpose=purpose,
        )

    def _compile(
        self,
        transform: type[Transform] | TransformPipeline,
        plan,
        *,
        target: str,
        configuration: Mapping[str, object],
        registry,
        plugin=None,
        purpose: CompilationPurpose = CompilationPurpose.RUNTIME,
    ) -> PluginCompilation:
        plugin = plugin or (registry or self._registry or Plugin.registry()).select(target)
        compilation = plugin.api.compiler.compile(
            CompileRequest(transform=transform, target=target, configuration=configuration, analysis=plan, purpose=purpose)
        )
        if not isinstance(compilation, PluginCompilation):
            raise ValueError(f"PLUGIN-E2708: Plugin {target!r} returned an invalid compilation result.")
        return replace(compilation, analysis=plan)

    @staticmethod
    def _validate_declared_schemas(
        transform: type[Transform] | TransformPipeline, plugin, configuration: Mapping[str, object]
    ) -> None:
        validate = getattr(plugin.api.schema, "validate", None)
        if not callable(validate):
            return
        schemas: dict[object, None] = {}
        for transform_class in CompilePluginTransform._transform_classes(transform):
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
        configuration = PluginConfiguration.resolve({"plugin": {"default": default}})
        return Plugin.resolve_target()(transform, configuration=configuration)
