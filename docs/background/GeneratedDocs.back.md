# Generated Documentation

Structure can generate Markdown and JSON reference artifacts during `structure compile`. These files are useful for
publishing schema and transform contracts in CI without asking readers to inspect generated PySpark.

Configure the destination and formats:

```toml
generated_docs = false
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
```

`generated_docs_dir` is inside `generated_dir`, so the default path is `generated/docs`.
Set `generated_docs = true` in config or pass `structure compile --generated-docs` to opt in.
When docs are disabled, `compile --fail-on-diff` ignores existing generated docs.

Programmatic integrations can call `Docs.render.project()` from `structure.app.docs.api`.

Generated documentation includes:

- `index.md` and/or `index.json` with discovered schemas and compiled transforms.
- `schemas/<Schema>.md` and/or `schemas/<Schema>.json`.
- `transforms/<module>.<Transform>.md` and/or `transforms/<module>.<Transform>.json`.

Schema pages list field names, Spark columns, Structure types, nullability, and primary-key flags. Transform pages list
inputs, outputs, step methods, dependencies, and target artifacts such as generated PySpark and traceability JSON.

JSON files are valid JSON and include `"generated_by": "Structure"`. Markdown files use the standard generated-file
ownership header.
