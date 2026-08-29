# Testing Helpers

Structure exposes reusable pytest-oriented helpers from `structure.lib.testing`. They are fixture-agnostic and safe to
import during Spark-free test collection. See [Testing](../dev/Testing.md) for the governing guidance.

Use these helpers in downstream projects to keep compiler and generated-code assertions terse:

```python
from structure.lib.testing import *


def test_structure_generated_output_is_current(tmp_path):
    assert_check_success(project_root=tmp_path)
    assert_compile_success(project_root=tmp_path)
    assert_generated_fresh(project_root=tmp_path)
    assert_generated_snapshot(tmp_path / "generated", generated_files("tests/snapshots/generated"))
```

## Test Strategy

The helpers cover four different contracts. Use the narrowest one that proves the behavior under test:

```text
source/configuration -> assert_check_success(...) -> compiler acceptance
source/configuration -> assert_compile_success(...) -> generated artifacts
generated tree       -> assert_generated_fresh(...) -> no unreviewed drift
online + generated   -> assert_online_generated_parity(...) -> execution equivalence
```

Negative tests use `assert_expected_diagnostic(...)`; artifact tests use `generated_files(...)` and
`assert_generated_snapshot(...)`. These helpers complement ordinary unit and integration tests. They do not replace
business-row assertions, live Spark tests, or tests for caller-owned storage and streaming orchestration.

The recommended progression for a feature is:

1. Assert that the source is accepted or that the expected diagnostic is emitted.
2. Assert the generated shape and any generated documentation that is part of the contract.
3. Assert online/generated parity for the supported runtime target.
4. Assert business results and edge cases at the appropriate data grain.

This keeps compiler failures, artifact drift, runtime parity, and business semantics distinguishable in test output.

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

Pass settings as keyword arguments or as an `overrides` mapping, but do not provide the same setting through both
forms. The helper rejects duplicate settings before invoking the CLI application. The return value is the command's
tuple of output lines, so a test can assert a high-signal line without snapshotting the whole terminal transcript:

```python
lines = assert_compile_success(project_root=project_root, generated_docs=True)
assert "Structure compile passed" in lines
```

`assert_generated_fresh(...)` forces `fail_on_diff=True`, compares would-be output with the configured generated tree,
and rewrites the failure as an actionable assertion. It does not update stale files. The intended repair is to run
`structure compile`, review the diff, and commit the generated changes when they are expected.

## Snapshot Helpers

`generated_files(root)` reads a generated tree into a `{path: text}` mapping, skipping Python cache files.

`assert_generated_snapshot(actual, expected)` compares either two file mappings or two directories. It reports missing,
extra, and changed files with a unified diff.

Snapshots should describe a reviewed artifact contract rather than every incidental byte in a temporary directory. A
useful pattern is to generate into a temporary project, read the tree once, and compare it with a checked-in mapping or
fixture directory:

```python
actual = generated_files(project_root / "generated")
expected = generated_files("tests/snapshots/generated")
assert_generated_snapshot(actual, expected)
```

`generated_files(...)` uses POSIX-relative paths, sorts traversal, and skips `__pycache__` and `.pyc` files. It returns
an empty mapping for a missing root, which makes a missing artifact fail as a clear snapshot difference instead of
raising an unrelated path error.

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

The helper returns the structured diagnostic, so a test can inspect fields such as source, severity, or related spans
when those are part of the contract. Prefer a stable code and a short fragment of the problem or fix over asserting an
entire rendered message. `source_endswith` is useful for source-order compiler tests because it avoids coupling the
test to a machine-specific absolute path.

## Parity Helpers

`assert_online_generated_parity(online, generated)` runs two callables and compares their outputs. The helper accepts a
single DataFrame-like result, a mapping of output names, or `TransformResult`-like objects with `as_dict()`.

For DataFrame-like outputs, it compares column order, collected rows, and schema text when available. Row order is
ignored by default because Spark DataFrames are unordered unless a transform explicitly sorts.

Use `outputs=(...)` to compare selected named outputs, `compare_schema=False` when the test intentionally covers only
row behavior, and `ordered=True` only when ordering is an explicit transform contract:

```python
assert_online_generated_parity(
    lambda: online_result,
    lambda: generated_result,
    outputs=("published",),
    ordered=True,
)
```

The helper normalizes mapping values, nested rows, and `TransformResult`-like objects with `as_dict()`. It does not
start Spark or decide whether a result is business-correct; both callables must construct their results using the
test's own session and fixtures.

## Failure Boundaries And Ownership

| Failure | Preferred helper | Repair ownership |
| --- | --- | --- |
| unsupported source or configuration | `assert_expected_diagnostic` | Structure source or configuration |
| expected source is rejected | `assert_check_success` | transform author |
| generated files are missing or wrong | `assert_generated_snapshot` | generated artifact and compiler |
| committed generated tree is stale | `assert_generated_fresh` | project regeneration workflow |
| online and generated results differ | `assert_online_generated_parity` | compiler, target plugin, or hook |
| business result is wrong | ordinary result assertions | feature transform and fixture |

Do not use a snapshot to conceal a semantic mismatch, and do not use parity to prove a ranking, allocation, or
aggregation policy is correct. First establish that both paths agree; then assert the domain-specific result explicitly.

## Spark-Free Collection Contract

The module is safe to import while pytest is collecting tests without a Spark session. Compiler, snapshot, and
diagnostic helpers are Spark-free; parity remains lazy and only touches Spark when the supplied callables do. A test
module may therefore import all helpers at module scope and create a session inside the test or fixture that needs it.

The helpers are intentionally pytest-oriented but do not require pytest internals. Their failures are ordinary
`AssertionError` or `ValueError` instances, allowing downstream projects to use them from custom test runners when the
same contracts are useful.

## Acceptance Contract

Testing helper support is complete when compiler success, compilation success, stale-artifact detection, generated
snapshot diffs, expected diagnostic matching, mapping and multi-output parity, schema comparison, unordered rows, and
ordered output checks are covered by tests. The helpers must retain actionable failure text while remaining independent
of Spark during import and configuration-only checks.
