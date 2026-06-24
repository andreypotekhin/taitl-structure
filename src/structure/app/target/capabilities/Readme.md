# Target Capabilities App

## Purpose
The target capabilities app describes what a backend can support and turns unsupported feature use into precise
diagnostics. It lets Structure validate target assumptions early instead of scattering version checks across compiler,
runtime, and generator code.

## Dependency Exchanges
The app consumes backend identifiers, configured version ranges, and `CapabilityRequirement` values from target
lowering. It returns `BackendCapabilities` implementations, `CapabilityDecision` records, generated import names, or
`BackendCapabilityError` failures backed by `BackendDiagnostic`.

The `capabilities` API endpoint exposes backend capability resolution as a fresh command factory:

```python
capabilities.resolve()
```

## Inner Workings
`ResolveBackendCapabilities` selects the capability implementation for the configured target, currently the
`PySparkCapabilities` rules object. Model classes such as `BackendId`, `CapabilityRequirement`, `CapabilityDecision`,
and `GeneratedImports` keep target checks data-driven and easy to reuse from configuration and PySpark lowering.
