from __future__ import annotations

from collections import defaultdict

from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.compiler.api import Compiler, CompilerOptions
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.platforms.api.Platform import Platform
from structure.platform.api.v1.GenerationRequest import GenerationRequest


class RenderConfiguredPlatformProject:

    def __init__(self, registry=None) -> None:
        self._registry = registry or Platform.registry()

    def __call__(
        self,
        config: StructureConfig,
        project: DiscoveredStructureProject,
        *,
        transforms: tuple[type[Transform], ...] | None = None,
    ) -> dict[str, str]:
        files: dict[str, str] = {}
        options = CompilerOptions.from_config(config)
        builder = Compiler.artifacts.build()
        platform = self._registry.select(config.target_backend)
        if platform.api.generator is None:
            raise ValueError(f"PLATFORM-E2709: Platform {config.target_backend!r} does not provide generation.")
        for source_module, group in self._source_units(transforms or project.transforms).items():
            plans = {}
            fingerprints = {}
            for transform in group:
                source_transform = f"{transform.__module__}.{transform.__name__}"
                artifact = builder(transform, options=options, materialize_schemas=False)
                plans[source_transform] = artifact.payload
                fingerprints[source_transform] = artifact.semantic_fingerprint
            files.update(
                platform.api.generator.generate(
                    GenerationRequest(
                        payload=plans,
                        source_module=source_module,
                        source_schema_modules=project.schema_modules,
                        generated_package=config.generated_package,
                        semantic_fingerprints=fingerprints,
                        generated_code_options=config.generated_code_options,
                    )
                )
            )
        return files

    def _source_units(
        self,
        transforms: tuple[type[Transform], ...],
    ) -> dict[str, tuple[type[Transform], ...]]:
        grouped: dict[str, list[type[Transform]]] = defaultdict(list)
        for transform in transforms:
            grouped[transform.__module__].append(transform)
        return {module: tuple(items) for module, items in grouped.items()}


render_configured_platform_project = RenderConfiguredPlatformProject()
