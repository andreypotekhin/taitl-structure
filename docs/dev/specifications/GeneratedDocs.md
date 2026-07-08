# Generated Documentation

## Purpose

Generated documentation artifacts let downstream projects publish Structure schema and transform reference material from
the same source that produces generated PySpark. The artifacts are adoption-facing contracts: they describe schemas,
inputs, outputs, subtransforms, dependencies, and generated targets without requiring readers to inspect generated code.

## Configuration

`structure compile` writes generated documentation under `generated_docs_dir`, which is relative to `generated_dir`.
The default destination is `generated/docs`.

`generated_docs_formats` controls formats:

- `markdown` writes human-readable reference pages.
- `json` writes equivalent machine-readable contract artifacts.

Default:

```toml
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
```

## Artifact Layout

For each compile run, Structure writes:

- `index.md` and/or `index.json` with links or summaries for all discovered schemas and compiled transforms.
- `schemas/<Schema>.md` and/or `schemas/<Schema>.json` for every discovered schema class.
- `transforms/<module>.<Transform>.md` and/or `transforms/<module>.<Transform>.json` for every compiled transform.

Markdown files start with the standard Structure ownership header. JSON files remain valid JSON and include
`"generated_by": "Structure"` so generated-file cleanup can identify them as owned artifacts.

## Schema Contract

Schema artifacts include:

- Schema name.
- Source module.
- Direct Structure schema bases.
- Fields in declaration order, including Python field name, Spark column name, Structure type, nullability, and primary
  key flag.

## Transform Contract

Transform artifacts include:

- Transform name and source class.
- Declared inputs and outputs.
- Subtransform names, lane/schema transitions, bound input parameters, and result lanes.
- Join dependencies where present.
- Target artifact paths for generated PySpark and traceability JSON.

The generated docs intentionally avoid private compiler internals. Traceability artifacts remain the richer machine
view for provenance and static dataflow.
