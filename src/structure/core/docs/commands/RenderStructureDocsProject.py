from __future__ import annotations

import json
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from typing import cast

from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.compiler.api.Compiler import Compiler
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.docs.logic.RenderStructureDocsMarkdown import RenderStructureDocsMarkdown
from structure.core.docs.logic.StructureDocsData import StructureDocsData
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.plugins.api.Plugin import Plugin
from structure.plugin.api.v1 import CompilationPurpose


class RenderStructureDocsProject:

    def __init__(self) -> None:
        self._data = StructureDocsData()
        self._markdown = RenderStructureDocsMarkdown()

    def __call__(
        self,
        config: StructureConfig,
        project: DiscoveredStructureProject,
        *,
        transforms: tuple[type[Transform], ...] | None = None,
    ) -> dict[str, str]:
        formats = set(config.generated_docs_formats)
        docs_root = self._docs_root(config)
        selected = transforms or project.transforms
        plugin = Plugin.registry().select(config.target)
        plans = {
            f"{transform.__module__}.{transform.__name__}": cast(
                TransformPlan,
                Compiler.frontend.compile()(
                    transform,
                    config=config,
                    materialize_schemas=False,
                    purpose=CompilationPurpose.DOCUMENTATION,
                ).analysis,
            )
            for transform in selected
        }
        analysis = plugin.api.analysis
        describe = None if analysis is None else getattr(analysis, "describe_documentation", None)
        if not callable(describe):
            raise ValueError(f"PLUGIN-E2709: Plugin {config.target!r} does not provide documentation analysis.")
        platform_details = {source: describe(plan) for source, plan in plans.items()}
        data = self._data.project(
            project,
            plans,
            platform_details=platform_details,
            traceability=config.traceability,
        )

        files: OrderedDict[str, str] = OrderedDict()
        if "markdown" in formats:
            files[f"{docs_root}/index.md"] = self._markdown.index(data)
            for schema in self._items(data, "schemas"):
                files[f"{docs_root}/schemas/{schema['name']}.md"] = self._markdown.schema(schema)
            for transform in self._items(data, "transforms"):
                files[f"{docs_root}/transforms/{transform['source']}.md"] = self._markdown.transform(transform)
        if "json" in formats:
            files[f"{docs_root}/index.json"] = self._json(data)
            for schema in self._items(data, "schemas"):
                files[f"{docs_root}/schemas/{schema['name']}.json"] = self._json(schema)
            for transform in self._items(data, "transforms"):
                files[f"{docs_root}/transforms/{transform['source']}.json"] = self._json(transform)
        return dict(files)

    def _docs_root(self, config: StructureConfig) -> str:
        return config.generated_docs_dir.relative_to(config.generated_dir).as_posix()

    def _json(self, data: object) -> str:
        if isinstance(data, dict):
            data = {"generated_by": "Structure", **data}
        return json.dumps(data, indent=2, sort_keys=True) + "\n"

    def _items(self, data: Mapping[str, object], key: str) -> Sequence[Mapping[str, object]]:
        return cast(Sequence[Mapping[str, object]], data[key])


render_structure_docs_project = RenderStructureDocsProject()
