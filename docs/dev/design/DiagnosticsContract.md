# Diagnostics Registry and Documentation Contract

Structure diagnostics need one owner before many checks are implemented. Each subsystem already knows what can go
wrong, but a user-facing library needs stable codes, stable links, severity rules, and tests that prove the useful
parts of each message stay useful.

This design resolves C29 by adding a registry-backed diagnostic contract.

## Problem

The current specifications ask for good diagnostics in many places: configuration, CLI, DSL, IR, joins, hooks,
backend capabilities, generated-code drift, and runtime execution. Without a registry, those diagnostics can drift:

- two checks can accidentally reuse the same code;
- examples can publish codes that implementation never emits;
- docs can move anchors that CI and IDE integrations depend on;
- tests can assert only that "an error happened" instead of asserting the actionable message fields;
- severity can vary by renderer instead of being part of the diagnostic itself.

## Design

Diagnostics are structured values created from a registry entry plus contextual fields gathered by the failing phase.
The registry owns code, severity, title, owner area, lifecycle status, documentation link, and message templates. The
phase owns concrete context such as transform name, setting path, source location, join occurrence, or generated file.

The public code format is:

```text
CONF-E0101
```

The prefix names the component that issued the diagnostic, and the severity letter is part of the code. Component
prefixes are documented in `docs/specifications/Diagnostics.md`. Both errors and warnings use the same component
prefix for their feature area.

`docs/Diagnostics.md` is the public index. Feature specs may include deeper examples, but public diagnostic links
should prefer a stable index anchor such as:

```text
docs/Diagnostics.md#conf-e0101
```

The CLI, runtime exceptions, CI annotations, and future IDE integrations are renderers. Renderers may change layout,
but they must preserve code, severity, title, problem, suggested fix, and docs link.

## Implementation Direction

Add a registry before broad diagnostic implementation. The first useful slice should include:

- unknown configuration key;
- invalid configuration value;
- unsupported symbolic expression;
- stale generated output;
- unknown transform input at runtime;
- unexpected internal failure.

This is enough to prove the registry across configuration, compiler, generated-output, runtime, and CLI surfaces.

Validation tests should fail on duplicate codes, malformed codes, missing docs links, unknown component prefixes, and
deprecated codes without replacement links. Diagnostic behavior tests should assert the code and high-signal fields,
not whole terminal blocks.

## Consequences

The project gains stable targets for tests, troubleshooting pages, CI annotations, and IDE diagnostics. The cost is a
little ceremony when adding a new diagnostic: a registry entry, a docs anchor, and a focused test. That ceremony is
worth it because diagnostics are part of the public developer experience.

Before 1.0, draft diagnostics may still move. Once a code is published in a release, changing its meaning requires a
new code or an explicit deprecation path.
