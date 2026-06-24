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

The `api/` package exposes the Click group as `cli` and command factories for internal CLI actions as `cli_actions`:

```python
cli_actions.discover_project()
cli_actions.render_configured_pyspark_project()
cli_actions.render_explain_report()
```

## Inner Workings
`api/cli.py` exposes the Click entry point and `StructureCliGroup`, while `DiscoverStructureProject` imports source
roots into a `DiscoveredStructureProject`. `RenderConfiguredPySparkProject` compiles and lowers every discovered
`Transform`, and `RenderExplainReport` assembles a concise compiler, target, streaming, and traceability view for one
transform.
