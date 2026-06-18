# Compatibility

Structure has three compatibility surfaces:

- the Structure source DSL and configuration users write;
- the generated PySpark code committed to user projects;
- optional metadata artifacts such as compiler provenance and static dataflow lineage.

This page defines the public compatibility policy for v1 and the planned versioning rules after the first stable
release.

## v1 Baseline

Structure v1 targets:

- Python 3.11 and newer;
- PySpark 3.5.x and 4.0.x, expressed as `target_pyspark = ">=3.5,<4.1"` by default;
- Linux runtime environments;
- Linux and macOS development environments;
- Airflow and other schedulers without a hard runtime dependency on them.

Windows development should remain usable where practical, but Linux is the v1 runtime target for generated Spark jobs.

## PySpark Targeting

Set the generated-code target in project configuration:

```toml
[tool.structure]
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
```

The `target_pyspark` value constrains which PySpark APIs the emitter may use. Generated code should avoid APIs outside
that range unless the user explicitly changes the target.

When a transform uses a feature that cannot be generated for the configured target, Structure should fail during
`structure check` or `structure compile` with a diagnostic that names the required PySpark version and points back to
this page.

## Spark Connect

Spark Connect is not a v1 or v2 commitment. v1 and v2 generated code targets ordinary PySpark `SparkSession`,
`DataFrame`, and `Column` APIs.

Spark Connect support is scheduled for v3 with streaming orchestration and backend expansion work. It may land earlier
only if it can be implemented through the existing PySpark emitter without changing public APIs, generated-code shape,
or compatibility guarantees.

## Semantic Versioning

After Structure reaches 1.0, public releases follow semantic versioning:

- `MAJOR` versions may change public DSL, configuration, runtime helper APIs, generated-code contracts, or supported
  Python/PySpark ranges.
- `MINOR` versions may add compatible DSL features, config keys, diagnostics, generated-code improvements, and support
  for newer Python or PySpark versions.
- `PATCH` versions should contain bug fixes, documentation fixes, and compatible diagnostic improvements.

Before 1.0, minor versions may still change public contracts, but each release should document migration steps.

Dropping a supported Python or PySpark line is normally a major-version change. A line that is already unsupported by
its upstream project may be dropped in a minor release if the release notes include a clear migration note.

## Generated-Code Compatibility

Generated PySpark is committed build output owned by the Structure compiler. Regenerate it after upgrading Structure.

Compatibility rules:

- Generated code should declare the Structure generator version and target PySpark range in a header comment.
- Generated code may depend on Structure runtime helpers only through documented generated-runtime APIs.
- Runtime helper breaking changes require either a major Structure version or a compatibility shim.
- CI should run `structure compile --fail-on-diff` after upgrades so generated-code changes are reviewed.

Generated code is readable and reviewable, but not hand-edited. Change source Structure code, configuration, or the
compiler instead.

## Compiler Lineage Schema Versioning

Compiler lineage covers two metadata models:

- compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
- static dataflow lineage, which records transform, table, and column dependencies inferred from IR.

Lineage schema rules:

- Breaking metadata-shape changes bump the lineage schema major version.
- Additive fields bump the lineage schema minor version.
- Consumers should ignore unknown fields.
- Structure should keep default compiler lineage compact and stable across patch releases.

Runtime LDJSON lineage is not part of the v1 compatibility contract. It is tracked as a nice-to-have beyond v3 in
`docs/dev/project-management/NiceToHave.md`.

## Config Schema Versioning

Configuration has an implicit schema version for v1. A future explicit key may make this visible:

```toml
config_schema_version = 1
```

Config schema rules:

- Unknown keys and invalid values are errors with structured diagnostics.
- New optional keys may be added in minor versions.
- Removing or changing the meaning of a documented key requires a major version after 1.0.
- Deprecated keys should produce warnings before removal when practical.

## Roadmap

v2 expands generated PySpark features while preserving the same basic compatibility contract.

v3 adds backend and orchestration work, including Spark Connect support when it can be specified, tested, and documented
without weakening the generated-code review model.
