# Configuration App

## Purpose
The configuration app resolves Structure's effective project settings from defaults, project files, and caller
overrides. It is the early validation boundary that catches under-configured paths, invalid modes, unknown settings,
and unsupported target requests before discovery or execution starts.

## Dependency Exchanges
The app consumes `pyproject.toml`, `structure.toml`, CLI overrides, filesystem paths, and backend target settings. It
returns a `StructureConfig` with source roots, generated paths, execution mode, validation defaults, traceability mode,
and capability decisions, or raises `ConfigError` carrying a `ConfigDiagnostic`.

The `configuration` API endpoint exposes configuration resolution as a fresh command factory:

```python
configuration.resolve()
```

## Inner Workings
`ResolveStructureConfig` merges settings by precedence, checks spelling and value domains, normalizes project-relative
paths, rejects dangerous generated package names, and asks target capabilities to validate configured backends.
`StructureConfig`, `ConfigDiagnostic`, and `ConfigError` keep the resolved result and failures easy to pass across app
boundaries.
