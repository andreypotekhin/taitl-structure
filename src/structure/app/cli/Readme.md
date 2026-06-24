# CLI App

## Purpose
The CLI app owns the command-line contract for Structure: `structure init`, `structure check`, `structure compile`,
`structure explain`, and `structure clean`. It turns terminal input into configured project operations and keeps the
user-facing boundary small, diagnostic, and resilient.

## Dependency Exchanges
The app receives configuration values from Click options, resolves them through the configuration app, discovers DSL
schemas and transforms through its own project scanner, compiles transforms through the DSL/compiler boundary, and
hands generated artifact work to the PySpark target app. It returns console text, `click.ClickException` diagnostics,
generated-file writes, diff failures, and explain reports.

The `api/` package exposes the Click group as `cli` and command factories for internal CLI work through `CliApp`:

```python
CliApp.resolve_config()
CliApp.check_project()
CliApp.compile_project()
CliApp.explain_transform()
CliApp.clean_generated_files()
```

## Inner Workings
`api/cli.py` exposes the Click entry point and `StructureCliGroup`, then delegates through `CliApp` to focused commands.
`DiscoverStructureProject` imports source roots into a `DiscoveredStructureProject`. `RenderConfiguredPySparkProject`
compiles and lowers every discovered `Transform`, and `RenderExplainReport` assembles a concise compiler, target,
streaming, and traceability view for one transform.
