from __future__ import annotations

import time
from pathlib import Path

import click

from structure.app.cli.api.CliApp import CliApp
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
    _echo(CliApp.write_config()(root=Path.cwd(), seed_config=seed_config))


@cli.command()
@click.option("--profile", is_flag=True)
@_config_options
def check(profile: bool, **kwargs) -> None:
    """Validate Structure source without writing generated files."""
    started = time.perf_counter()
    config = CliApp.resolve_config()(kwargs)
    _echo(CliApp.check_project()(config))
    if profile:
        _profile(started)


@cli.command()
@click.option("--profile", is_flag=True)
@_config_options
def compile(profile: bool, **kwargs) -> None:
    """Generate PySpark artifacts."""
    started = time.perf_counter()
    config = CliApp.resolve_config()(kwargs)
    _echo(CliApp.compile_project()(config))
    if profile:
        _profile(started)


@cli.command()
@_config_options
@click.argument("transform")
def explain(transform: str, **kwargs) -> None:
    """Explain one transform."""
    config = CliApp.resolve_config()(kwargs)
    _echo(CliApp.explain_transform()(config, transform))


@cli.command()
@_config_options
def clean(**kwargs) -> None:
    """Remove Structure-owned generated artifacts."""
    config = CliApp.resolve_config()(kwargs)
    _echo(CliApp.clean_generated_files()(config))


def _echo(lines: tuple[str, ...]) -> None:
    for line in lines:
        click.echo(line)


def _profile(started: float) -> None:
    elapsed = int((time.perf_counter() - started) * 1000)
    click.echo("Profile")
    click.echo(f"  total: {elapsed} ms")


if __name__ == "__main__":
    cli()
