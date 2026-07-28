# Composed Hook Ownership

Transform composition may include hook-bearing stages only after hook ownership is explicit. The current supported
composition surface remains hook-free.

## Decision

A composed transform does not own hooks declared by its source stages. Each hook belongs to the transform class where it
is declared, runs on an instance of that source transform, and keeps that transform's lane names, validation boundaries,
source locations, traceability records, and streaming compatibility classification.

Generated code must construct one implementation object per hook-bearing source stage. It must import the declaring
class directly, call hooks in the same lifecycle position they would occupy when the source transform runs alone, and
validate the lane schema immediately before and after the hook according to the hook declaration.

Composition wrappers may route declared outputs between stages, but they must not match `lane(...)` as a public
composition boundary. A lane remains internal to the declaring transform unless a later specification introduces an
explicit public intermediate-output contract.

## Rules

- Source-stage step methods and hooks run in source order within that stage.
- Cross-stage order follows the declared `.to(...)` composition graph.
- Hooks execute on their declaring stage instance, not on the wrapper.
- Generated `_impl` construction is per declaring class, with stable imports and no shared mutable hook delegate.
- Traceability records the wrapper composition edge and the source-stage hook boundary.
- Streaming reporting is the minimum compatibility of every composed stage and hook.
- A hook-bearing stage cannot be composed until generated and online execution have parity tests for the hook order,
  validation boundary, traceability, and streaming report.

## Deferred Questions

Wrapper-local hooks, wrapper-local step methods, and exposing earlier-stage outputs from a composed wrapper remain
deferred. They need a separate public intermediate-output contract rather than overloading existing lane internals.
