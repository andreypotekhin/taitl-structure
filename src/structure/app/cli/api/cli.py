from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path
from typing import cast

import click

from structure.app.cli.logic.actions.DiscoverStructureProject import discover_structure_project
from structure.app.cli.logic.actions.RenderConfiguredPySparkProject import render_configured_pyspark_project
from structure.app.cli.logic.actions.RenderExplainReport import render_explain_report
from structure.app.configuration.api import ConfigError, StructureConfig, resolve_structure_config
from structure.app.target.pyspark.api import compare_generated_files, write_generated_files
from structure.lib.cross.errors import Diagnostic, diagnostic_registry, render_diagnostic


def _config_options(function):
    options = [
        click.option("--source-root", "source_roots", multiple=True),
        click.option("--generated-dir"),
        click.option("--generated-package"),
        click.option("--execution-mode", type=click.Choice(["online", "generated"])),
        click.option("--target-backend"),
        click.option("--target-pyspark"),
        click.option("--traceability", type=click.Choice(["none", "compiler", "columns", "debug"])),
        click.option("--fail-on-diff", is_flag=True, default=None),
    ]
    for option in reversed(options):
        function = option(function)
    return function


class StructureCliGroup(click.Group):

    def invoke(self, ctx):
        try:
            return super().invoke(ctx)
        except click.ClickException:
            raise
        except Exception as error:
            raise self._internal_error(error)

    def main(self, *args, **kwargs):
        try:
            return super().main(*args, **kwargs)
        except click.ClickException:
            raise
        except Exception as error:
            raise self._internal_error(error)

    def _internal_error(self, error: Exception) -> click.ClickException:
        diagnostic = Diagnostic(
            entry=diagnostic_registry["CLI-X1101"],
            problem="An unclassified exception reached the CLI boundary.",
            use=diagnostic_registry["CLI-X1101"].use_template,
            context={"exception": type(error).__name__},
        )
        return click.ClickException(render_diagnostic(diagnostic, kind="CLIError"))


@click.group(cls=StructureCliGroup)
def cli() -> None:
    """Structure compiler commands."""


@cli.command()
@click.option("--seed-config", is_flag=True)
def init(seed_config: bool) -> None:
    """Create Structure configuration."""
    root = Path.cwd()
    pyproject = root / "pyproject.toml"
    structure = root / "structure.toml"
    if pyproject.exists():
        text = pyproject.read_text(encoding="utf-8")
        if "[tool.structure]" in text:
            raise click.ClickException("Structure configuration already exists in pyproject.toml")
        pyproject.write_text(text.rstrip() + "\n\n" + _seed_config(seed_config), encoding="utf-8")
        click.echo("Wrote pyproject.toml [tool.structure]")
        return
    if structure.exists():
        raise click.ClickException("Structure configuration already exists in structure.toml")
    structure.write_text(_seed_config(seed_config), encoding="utf-8")
    click.echo("Wrote structure.toml")


@cli.command()
@click.option("--profile", is_flag=True)
@_config_options
def check(profile: bool, **kwargs) -> None:
    """Validate Structure source without writing generated files."""
    started = time.perf_counter()
    config = _config(kwargs)
    project = discover_structure_project(config)
    for transform in project.transforms:
        render_explain_report(transform)
    click.echo("Structure check passed")
    click.echo(f"  source roots: {', '.join(_relative(config, root) for root in config.source_roots)}")
    click.echo(f"  transforms: {len(project.transforms)}")
    click.echo(f"  schemas: {len(project.schemas())}")
    if profile:
        _profile(started)


@cli.command()
@click.option("--profile", is_flag=True)
@_config_options
def compile(profile: bool, **kwargs) -> None:
    """Generate PySpark artifacts."""
    started = time.perf_counter()
    overrides = dict(kwargs)
    config = _config(overrides)
    project = discover_structure_project(config)
    files = render_configured_pyspark_project(config, project)
    if config.fail_on_diff:
        result = compare_generated_files(files, root=config.generated_dir)
        if result.changed():
            lines = "\n".join(
                f"{change.status:8} {change.path}" for change in result.changes if change.status != "unchanged"
            )
            diagnostic = Diagnostic(
                entry=diagnostic_registry["GEN-E0901"],
                problem="Generated output differs from current Structure source or configuration.",
                use=diagnostic_registry["GEN-E0901"].use_template,
                context={"generated_dir": _relative(config, config.generated_dir), "changes": lines},
            )
            raise click.ClickException(render_diagnostic(diagnostic, kind="GeneratedOutputError"))
    else:
        result = write_generated_files(files, root=config.generated_dir)
    click.echo("Structure compile passed")
    click.echo(f"  generated dir: {_relative(config, config.generated_dir)}")
    click.echo(f"  transforms: {len(project.transforms)}")
    click.echo(f"  files written: {result.count('added') + result.count('modified')}")
    click.echo(f"  files unchanged: {result.count('unchanged')}")
    if profile:
        _profile(started)


@cli.command()
@_config_options
@click.argument("transform")
def explain(transform: str, **kwargs) -> None:
    """Explain one transform."""
    config = _config(kwargs)
    for root in config.source_roots:
        text = str(root)
        if text not in sys.path:
            sys.path.insert(0, text)
    module_name, name = transform.rsplit(".", 1)
    module = importlib.import_module(module_name)
    click.echo(render_explain_report(getattr(module, name)))


@cli.command()
@_config_options
def clean(**kwargs) -> None:
    """Remove Structure-owned generated artifacts."""
    config = _config(kwargs)
    if not config.generated_dir.exists():
        click.echo("Structure clean passed")
        click.echo("  removed files: 0")
        return
    removed = 0
    for path in sorted(item for item in config.generated_dir.rglob("*") if item.is_file()):
        if _owned(path):
            path.unlink()
            removed += 1
    for path in sorted((item for item in config.generated_dir.rglob("*") if item.is_dir()), reverse=True):
        try:
            path.rmdir()
        except OSError:
            pass
    click.echo("Structure clean passed")
    click.echo(f"  removed files: {removed}")


def _config(kwargs: dict[str, object]) -> StructureConfig:
    overrides = {key: value for key, value in kwargs.items() if value not in (None, (), False)}
    if "source_roots" in overrides:
        overrides["source_roots"] = list(cast(tuple[str, ...], overrides["source_roots"]))
    try:
        return resolve_structure_config(overrides=overrides)
    except ConfigError as error:
        raise click.ClickException(error.diagnostic.render()) from error


def _seed_config(seed: bool) -> str:
    lines = [
        "[tool.structure]",
        'source_roots = ["src"]',
        'generated_dir = "generated"',
        'generated_package = "structure_generated"',
        'execution_mode = "online"',
        'target_backend = "pyspark"',
        'target_pyspark = ">=3.5,<4.1"',
        'traceability = "compiler"',
    ]
    if seed:
        lines.extend(
            [
                "validate_inputs = true",
                'input_validation_mode = "schema_only"',
                "validate_intermediate = true",
                'intermediate_validation_mode = "schema_only"',
                "validate_outputs = true",
                'output_validation_mode = "schema_only"',
                "strict_performance = true",
                "fail_on_diff = false",
            ]
        )
    return "\n".join(lines) + "\n"


def _relative(config: StructureConfig, path: Path) -> str:
    try:
        return path.relative_to(config.project_root).as_posix()
    except ValueError:
        return path.as_posix()


def _owned(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False
    if text.startswith("# Generated by Structure. Do not edit by hand."):
        return True
    return path.suffix == ".json" and '"source_transform"' in text and '"generated_transform_class"' in text


def _profile(started: float) -> None:
    elapsed = int((time.perf_counter() - started) * 1000)
    click.echo("Profile")
    click.echo(f"  total: {elapsed} ms")


if __name__ == "__main__":
    cli()
