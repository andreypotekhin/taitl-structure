from __future__ import annotations

import time
from pathlib import Path

import click

from structure.app.cli.api.CliApp import CliApp
from structure.app.tools.api import StructureTools
from structure.app.tools.model import StructureToolError
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


@cli.group()
def tools() -> None:
    """End-user aid tools."""


@tools.group()
def schemas() -> None:
    """Schema aid tools."""


@schemas.command()
@click.option("--from-path", "from_path")
@click.option("--from-table", "from_table")
@click.option("--format", "format")
@click.option("--option", "options", multiple=True)
@click.option("--to", "to", required=True)
def generate(
    from_path: str | None, from_table: str | None, format: str | None, options: tuple[str, ...], to: str
) -> None:
    """Generate Structure schema classes."""
    try:
        sources = sum(source is not None for source in (from_path, from_table))
        reader_options = _reader_options(options)
        needs_spark = (
            sources == 1
            and (from_table is not None or format in {"parquet", "delta"})
            and (not reader_options or from_path is not None)
        )
        text = StructureTools.schemas.generate(
            from_path=from_path,
            from_table=from_table,
            format=format,
            spark=_spark_session() if needs_spark else None,
            options=reader_options,
            to=to,
        )
    except StructureToolError as error:
        raise click.ClickException(str(error))
    click.echo(text, nl=False)


def _echo(lines: tuple[str, ...]) -> None:
    for line in lines:
        click.echo(line)


def _reader_options(options: tuple[str, ...]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for option in options:
        if "=" not in option:
            raise click.ClickException(f"Invalid --option {option!r}. Use KEY=VALUE.")
        key, value = option.split("=", 1)
        if not key:
            raise click.ClickException(f"Invalid --option {option!r}. Use KEY=VALUE.")
        parsed[key] = value
    return parsed


def _spark_session():
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except Exception as error:
        raise click.ClickException(
            "Schema generation from paths or tables requires a Spark-available CLI runtime. "
            "Install/configure PySpark for this shell, or use StructureTools.schemas.generate(...) "
            "inside an existing Spark application with session=StructureSession(spark=spark). "
            "See docs/Troubleshooting.md."
        ) from error

    try:
        return SparkSession.builder.getOrCreate()
    except Exception as error:
        raise click.ClickException(
            "Spark could not start for schema generation. Run this command in a Spark-capable shell, "
            "or use the Python API inside an existing Spark application with session=StructureSession(spark=spark). "
            "See docs/Troubleshooting.md."
        ) from error


def _profile(started: float) -> None:
    elapsed = int((time.perf_counter() - started) * 1000)
    click.echo("Profile")
    click.echo(f"  total: {elapsed} ms")


if __name__ == "__main__":
    cli()
