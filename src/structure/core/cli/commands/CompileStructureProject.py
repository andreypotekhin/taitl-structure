from __future__ import annotations

from pathlib import Path

import click

from structure.core.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.core.cli.commands.RenderConfiguredPluginProject import RenderConfiguredPluginProject
from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.compiler.api.Compiler import Compiler
from structure.core.compiler.artifacts.model.GeneratedFileSetResult import GeneratedFileSetResult
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.docs.api import Docs
from structure.lib.cross.errors import Diagnostic, diagnostic_registry, render_diagnostic


class CompileStructureProject:

    def __init__(self) -> None:
        self._platform = RenderConfiguredPluginProject()

    def __call__(self, config: StructureConfig) -> tuple[str, ...]:
        project = DiscoverStructureProject()(config)
        files = self._platform(config, project) | self._docs(config, project)
        result = (
            self._compare(config, files)
            if config.fail_on_diff
            else Compiler.artifacts.write()(files, root=config.generated_dir)
        )
        return (
            "Structure compile passed",
            f"  generated dir: {self._relative(config, config.generated_dir)}",
            self._docs_summary(config),
            f"  transforms: {len(project.transforms)}",
            f"  files written: {result.count('added') + result.count('modified')}",
            f"  files unchanged: {result.count('unchanged')}",
        )

    def _docs(self, config: StructureConfig, project: DiscoveredStructureProject) -> dict[str, str]:
        if not config.generated_docs:
            return {}
        return Docs.render.project()(config, project)

    def _compare(self, config: StructureConfig, files: dict[str, str]) -> GeneratedFileSetResult:
        result = Compiler.artifacts.compare()(
            files,
            root=config.generated_dir,
            ignore_prefixes=self._compare_ignore_prefixes(config),
        )
        if result.changed():
            lines = "\n".join(
                f"{change.status:8} {change.path}" for change in result.changes if change.status != "unchanged"
            )
            diagnostic = Diagnostic(
                entry=diagnostic_registry["GEN-E0901"],
                problem="Generated output differs from current Structure source or configuration.",
                use=diagnostic_registry["GEN-E0901"].use_template,
                context={"generated_dir": self._relative(config, config.generated_dir), "changes": lines},
            )
            raise click.ClickException(render_diagnostic(diagnostic, kind="GeneratedOutputError"))
        return result

    def _compare_ignore_prefixes(self, config: StructureConfig) -> tuple[str, ...]:
        if config.generated_docs:
            return ()
        return (config.generated_docs_dir.relative_to(config.generated_dir).as_posix() + "/",)

    def _docs_summary(self, config: StructureConfig) -> str:
        if not config.generated_docs:
            return "  generated docs: disabled"
        return f"  generated docs dir: {self._relative(config, config.generated_docs_dir)}"

    def _relative(self, config: StructureConfig, path: Path) -> str:
        try:
            return path.relative_to(config.project_root).as_posix()
        except ValueError:
            return path.as_posix()
