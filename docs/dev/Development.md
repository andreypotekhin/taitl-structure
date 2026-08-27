# Development

## Project overview
[Overview.md](../Overview.md)

## Terminology
See [Terminology.md](Terminology.md) for project language.

See [Concepts.md](Concepts.md) for the
concept-test coverage map.

## Architecture
Main: [Architecture.md](Architecture.md)

Design gates: [gated/](gated/); deferred design: [deferred/](deferred/)

Design topic docs: **/docs/dev/design**

Specifications: **/docs/dev/specifications**

## Setup
See [Setup.md](Setup.md) for project setup and prerequisites.

## Building
### Prerequisites

    Python 3.12+
    Poetry (prefer Pipx installation)
    make

### Build project

    cd [project]
    make help
    make install
    make build

## Coding

Code structure: [Code.md](Code.md)

Coding style: [Style.md](Style.md)

## Default Project Layout

For execution (default):

```text
src/
  my_package/
    schemas/
    transforms/
```

For code generation and generated-code execution:

```text
src/
  my_package/
    schemas/
    transforms/
generated/
  structure_generated/
    my_package/
      pyspark/
        schemas/
        transforms/
    runtime/
    traceability/  # compiler metadata, not runtime telemetry
```

For example, `src/my_package/` generates `generated/structure_generated/my_package/pyspark/`.

Mark both `src` and `generated` as source roots in the IDE. The paths and package names are configurable.

## Targets and Plugins

Structure's public `structure` package contains target-neutral schema, transform, configuration, and session APIs.
Target DSLs are owned by plugins. The PySpark DSL is imported from `structure.plugin.pyspark`; configured under `[tool.structure.plugin.pyspark]`. Structure selects one plugin target for each
transform, then orchestrates compilation, execution, generation, artifacts, and diagnostics through its versioned
Plugin API. Use `@transform(target="...")`, a session or command `target=`, or `plugin.default` to select it.

An external plugin may use its own import package and DSL. Different transforms in one project can select different
installed plugins, but one composed pipeline always uses one target. See [Configuration.md](../Configuration.md) and
[Plugin Authoring](PluginAuthoring.md).

## Testing

Main: [Testing.md](Testing.md)

Testing guidelines: [Style.md](Style.md)

## Support and Contributions

We use a contributor-led support model suited to a developer audience. A code-related issue must include all of the
following:

- A minimal runnable code example, including the dependency and runtime versions needed to run it.
- The complete observed output, including an error and traceback when present.
- The expected output or behavior.
- A pull request with the proposed fix and a regression test derived from the example.

The issue establishes a reproducible contract; the pull request makes the remedy reviewable and testable. Requests
that cannot be reproduced or do not include a proposed fix remain incomplete. Non-code questions, documentation
corrections, and feature proposals do not need a runnable reproduction, but should state their use case precisely.

For work tracked inside this repository, create an [issue record](issues/Readme.md). The record is an automation-ready
copy of the report and points to its pull request. Do not put credentials, customer data, or other sensitive material
in either artifact.

## Troubleshooting
[Troubleshooting.md](Troubleshooting.md)
