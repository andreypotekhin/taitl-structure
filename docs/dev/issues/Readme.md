# Issue Records

Issue records are the repository-local, automation-ready representation of reproducible code issues. Keep one open
record per issue in this directory. Move resolved records to [done/](done/).

Use the same action naming and content conventions as suggestions and TODO items:

    IYYMMDDNN.concise-issue-title.md

For example, `I07152601.Generated-source-omits-nested-alias.md` is the first issue recorded on July 15, 2026. Each
file contains one H3 heading with the identifier and title, followed by the fields below. Keep lines within 120
characters and remove or anonymize sensitive data.

~~~md
### I07152601 Generated Source Omits Nested Alias

Status: open
Scope: generated PySpark source
Versions: Structure 4.0.0, Python 3.12, PySpark 4.0.0

Reproduction:
```python
# Minimal runnable example, including setup and invocation.
```

Observed output:
```text
# Complete output, including traceback when applicable.
```

Expected behavior:

# Describe the result that should occur.

Proposed fix:

- PR: https://example.invalid/owner/repo/pull/123
- Regression test: tests/specifications/example/test_nested_alias.py
~~~

An open code issue is actionable only when every field is present. The pull request must contain the proposed fix and
the regression test. Automation may use only the record's stated reproduction, expected behavior, and proposed-fix
links; it must not guess missing requirements.
