# Concept Coverage Map

Concept tests are public-facing end-to-end tests for the project vocabulary in `docs/dev/Concepts.md`.

Starting coverage is intentionally focused on concepts visible to Structure users:

- configuration: `tests/specifications/config-schema`, `tests/specifications/cli`
- schema: `tests/user_stories/05_schemas`, `tests/golden`
- transform invocation: `tests/user_stories/06_transform_classes`, `tests/specifications/online-execution`
- expressions and filtering: `tests/user_stories/11_symbolic_execution`, `tests/user_stories/13_filtering`
- projection: `tests/user_stories/14_add_and_drop_columns`
- joins: `tests/user_stories/15_joins`
- hooks: `tests/user_stories/16_hooks`
- generated PySpark: `tests/golden`, `tests/specifications/pyspark-code-generation`
- online execution: `tests/specifications/online-execution`
- diagnostics: `tests/user_stories/20_error_reporting`, `tests/specifications/compiler-diagnostics`
- compatibility: `tests/specifications/compatibility`, `tests/integration`

Prefer black-box tests through public APIs, CLI commands, generated code, diagnostics, and online/generated parity.
Avoid asserting compiler internals unless the public concept is the generated artifact or traceability report itself.
