# Testing Helpers

Structure exposes reusable pytest-oriented helpers from `structure.lib.testing`. They are fixture-agnostic and safe to
import in Spark-free test collection. The governing testing guidance is [Testing](../dev/Testing.md).

Use these helpers in downstream projects to keep compiler and generated-code assertions terse:

```python
from structure.lib.testing import *


def test_structure_generated_output_is_current(tmp_path):
    assert_check_success(project_root=tmp_path)
    assert_compile_success(project_root=tmp_path)
    assert_generated_fresh(project_root=tmp_path)
    assert_generated_snapshot(tmp_path / "generated", generated_files("tests/snapshots/generated"))
```

## Compiler Helpers

`assert_check_success(project_root=...)` resolves Structure configuration, runs the same project check used by the CLI,
and fails if the check does not report success.

`assert_compile_success(project_root=...)` runs generation and fails if compilation does not report success.

`assert_generated_fresh(project_root=...)` runs compile in fail-on-diff mode and raises an `AssertionError` with a
remedy when generated output is stale.

Each compiler helper accepts the same programmatic configuration overrides as `StructureConfig.resolve(...)`:

```python
assert_generated_fresh(
    project_root=".",
    generated_docs=False,
    target_profile=">=3.5,<4.1",
)
```

## Snapshot Helpers

`generated_files(root)` reads a generated tree into a `{path: text}` mapping, skipping Python cache files.

`assert_generated_snapshot(actual, expected)` compares either two file mappings or two directories. It reports missing,
extra, and changed files with a unified diff.

## Diagnostic Helpers

`assert_expected_diagnostic(action, code, ...)` runs a callable and asserts that it raises a Structure exception with
the expected diagnostic code. Optional `problem_contains`, `use_contains`, and `source_endswith` checks keep negative
compiler tests focused on user-facing guidance.

```python
diagnostic = assert_expected_diagnostic(
    lambda: compile_transform(BadTransform),
    "SCHEMA-E0301",
    problem_contains="may assign nullable value",
    use_contains="coalesce",
)
```

## Parity Helpers

`assert_online_generated_parity(online, generated)` runs two callables and compares their outputs. The helper accepts a
single DataFrame-like result, a mapping of output names, or `TransformResult`-like objects with `as_dict()`.

For DataFrame-like outputs, it compares column order, collected rows, and schema text when available. Row order is
ignored by default because Spark DataFrames are unordered unless a transform explicitly sorts.
