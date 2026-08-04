# Generated Documentation

## Purpose

Generated documentation artifacts let downstream projects publish Structure schema and transform reference material from
the same source that produces generated PySpark. The artifacts are adoption-facing contracts: they describe schemas,
inputs, outputs, step methods, dependencies, and generated targets without requiring readers to inspect generated code.

## Configuration

`structure compile` writes generated documentation under `generated_docs_dir`, which is relative to `generated_dir`.
The default destination is `generated/docs`.

Generated documentation is opt-in. Set `generated_docs = true` or pass `structure compile --generated-docs` to write
documentation artifacts. When
docs are disabled, `compile --fail-on-diff` ignores existing files under `generated_docs_dir` so teams can opt out
without removing old docs in the same change.

`generated_docs_formats` controls formats:

- `markdown` writes human-readable reference pages.
- `json` writes equivalent machine-readable contract artifacts.

Default:

```toml
generated_docs = false
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
```

## API

Application code can render docs through `Docs.render.project()` from `structure.app.docs.api`. The endpoint returns a
fresh `RenderStructureDocsProject` command instance and follows the same command-group style as other Structure app
endpoints.

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
- Step method names, lane/schema transitions, bound input parameters, and result lanes.
- Join dependencies where present.
- Target artifact paths for generated PySpark and traceability JSON.

The generated docs intentionally avoid private compiler internals. Traceability artifacts remain the richer machine
view for provenance and static dataflow.
