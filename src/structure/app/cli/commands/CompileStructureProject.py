from __future__ import annotations

from pathlib import Path

import click

from structure.app.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.app.cli.commands.RenderConfiguredPySparkProject import RenderConfiguredPySparkProject
from structure.app.configuration.model.StructureConfig import StructureConfig
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.model.GeneratedFileSetResult import GeneratedFileSetResult
from structure.lib.cross.errors import Diagnostic, diagnostic_registry, render_diagnostic


class CompileStructureProject:

    def __call__(self, config: StructureConfig) -> tuple[str, ...]:
        project = DiscoverStructureProject()(config)
        files = RenderConfiguredPySparkProject()(config, project)
        result = (
            self._compare(config, files)
            if config.fail_on_diff
            else PySpark.files.write()(files, root=config.generated_dir)
        )
        return (
            "Structure compile passed",
            f"  generated dir: {self._relative(config, config.generated_dir)}",
            f"  transforms: {len(project.transforms)}",
            f"  files written: {result.count('added') + result.count('modified')}",
            f"  files unchanged: {result.count('unchanged')}",
        )

    def _compare(self, config: StructureConfig, files: dict[str, str]) -> GeneratedFileSetResult:
        result = PySpark.files.compare()(files, root=config.generated_dir)
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

    def _relative(self, config: StructureConfig, path: Path) -> str:
        try:
            return path.relative_to(config.project_root).as_posix()
        except ValueError:
            return path.as_posix()
