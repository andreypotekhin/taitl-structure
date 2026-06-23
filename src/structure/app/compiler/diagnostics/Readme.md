# Compiler Diagnostics App

## Purpose
The compiler diagnostics app owns compile-time exception shape. It keeps invalid DSL source failures distinct from
configuration, runtime, and target capability failures.

## Dependency Exchanges
The app consumes cross-library `Diagnostic` values and exposes `StructureCompileError` through
`structure.app.compiler.diagnostics.api`. Compiler frontend code raises this error when source classes, signatures,
lanes, schemas, or symbolic expressions violate the DSL contract.

## Inner Workings
`StructureCompileError` is deliberately thin: it stores a diagnostic and renders through the shared diagnostic
renderer. Rich problem text and remediation live at the call site that understands the failing compiler rule.
