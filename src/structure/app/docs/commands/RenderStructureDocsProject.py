from __future__ import annotations

import json
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from typing import cast

from structure.app.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.app.compiler.api import Compiler
from structure.app.compiler.ir.model.TransformPlan import TransformPlan
from structure.app.configuration.model.StructureConfig import StructureConfig
from structure.app.docs.logic.RenderStructureDocsMarkdown import RenderStructureDocsMarkdown
from structure.app.docs.logic.StructureDocsData import StructureDocsData
from structure.app.dsl.model.transforms.Transform import Transform


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
        plans = {
            f"{transform.__module__}.{transform.__name__}": Compiler.frontend.compile()(transform)
            for transform in selected
        }
        data = self._data.project(project, plans)

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
