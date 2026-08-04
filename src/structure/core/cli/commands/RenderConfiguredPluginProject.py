from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping

from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.compiler.api.Compiler import Compiler
from structure.core.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.plugins.api.Plugin import Plugin
from structure.plugin.api.v1.model import GenerationRequest


class RenderConfiguredPluginProject:

    def __init__(self, registry=None) -> None:
        self._registry = registry or Plugin.registry()

    def __call__(
        self,
        config: StructureConfig,
        project: DiscoveredStructureProject,
        *,
        transforms: tuple[type[Transform], ...] | None = None,
    ) -> dict[str, str]:
        files: dict[str, str] = {}
        owners: dict[str, str] = {}
        options = CompilerOptions.from_config(config)
        builder = Compiler.artifacts.build()
        plugin = self._registry.select(config.target)
        if plugin.api.generator is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {config.target!r} does not provide generation.")
        for source_module, group in self._source_units(transforms or project.transforms).items():
            plans = {}
            fingerprints = {}
            for transform in group:
                source_transform = f"{transform.__module__}.{transform.__name__}"
                artifact = builder(transform, options=options, materialize_schemas=False)
                plans[source_transform] = artifact.payload
                fingerprints[source_transform] = artifact.semantic_fingerprint
            generated = plugin.api.generator.generate(
                GenerationRequest(
                    payload=plans,
                    source_module=source_module,
                    source_schema_modules=project.schema_modules,
                    generated_package=config.generated_package,
                    semantic_fingerprints=fingerprints,
                    generated_code_options=config.generated_code_options,
                    generated_code_hard_wrap=config.generated_code_hard_wrap,
                    traceability=config.traceability,
                )
            )
            self._merge_files(files, generated.files, source_module=source_module, owners=owners)
        return files

    def _merge_files(
        self,
        files: dict[str, str],
        generated: Mapping[str, str],
        *,
        source_module: str,
        owners: dict[str, str],
    ) -> None:
        for path, text in generated.items():
            previous = files.get(path)
            if previous is not None and previous != text:
                previous_owner = owners[path]
                raise ValueError(
                    f"PLUGIN-E2710: Generated file path collision at {path!r} between source modules "
                    f"{previous_owner!r} and {source_module!r}; preserve the full source module in the "
                    "plugin-generated path."
                )
            files[path] = text
            owners.setdefault(path, source_module)

    def _source_units(
        self,
        transforms: tuple[type[Transform], ...],
    ) -> dict[str, tuple[type[Transform], ...]]:
        grouped: dict[str, list[type[Transform]]] = defaultdict(list)
        for transform in transforms:
            grouped[transform.__module__].append(transform)
        return {module: tuple(items) for module, items in grouped.items()}


render_configured_platform_project = RenderConfiguredPluginProject()
