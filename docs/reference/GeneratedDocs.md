# Generated Documentation

Structure can generate Markdown and JSON reference artifacts during `structure compile`. These files are useful for
publishing schema and transform contracts in CI without asking readers to inspect generated PySpark.

Configure the destination and formats:

```toml
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
```

`generated_docs_dir` is inside `generated_dir`, so the default path is `generated/docs`.

Generated documentation includes:

- `index.md` and/or `index.json` with discovered schemas and compiled transforms.
- `schemas/<Schema>.md` and/or `schemas/<Schema>.json`.
- `transforms/<module>.<Transform>.md` and/or `transforms/<module>.<Transform>.json`.

Schema pages list field names, Spark columns, Structure types, nullability, and primary-key flags. Transform pages list
inputs, outputs, subtransforms, dependencies, and target artifacts such as generated PySpark and traceability JSON.

JSON files are valid JSON and include `"generated_by": "Structure"`. Markdown files use the standard generated-file
ownership header.
