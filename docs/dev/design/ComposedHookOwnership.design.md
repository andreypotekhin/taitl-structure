# Composed Hook Ownership

Transform composition may include hook-bearing stages because hook ownership is explicit. This applies equally to
invocation-level `.to(...)` pipelines and class-body dependency graphs. Authored graphs use bare transform assignments;
the explicit `stage(...)` helper remains a compatibility form. Hook-free composition remains supported, and hook-bearing
stages follow the owner-retention rules below.

## Decision

A composed transform does not own hooks declared by its source stages. Each hook belongs to the transform class where it
is declared, runs on an instance of that source transform, and keeps that transform's lane names, validation boundaries,
source locations, traceability records, and streaming compatibility classification.

Generated code follows the caller's `generated_code_options`. Without `embed_hooks`, it constructs one implementation
object per hook-bearing source stage, imports the declaring class directly, and calls each raw hook in its original
source-order position. With `embed_hooks`, it copies each eligible raw-hook body into the generated wrapper using a
stage- and owner-qualified generated name; no source-stage implementation object is created for that embedded hook.
Both modes validate the lane schema at the original raw-hook boundary. The online runner always uses the stage-owned
source instance model; embedding changes generated-source packaging, not online behavior.

Composition wrappers may route declared outputs between stages and may expose those declared outputs through the
recursive `TransformResult.stages` namespace. They must not match `lane(...)` as a public composition boundary. A lane
remains internal to the declaring transform, as do raw-hook frames.

## Implementation Contract

`ComposeTransformPlans` and `ComposeTransformGraph` must retain each stage's hook declarations and their declaring
owner instead of rejecting the stage. The composed plan records the stage ordinal or graph-stage name, declared hook
owner, hook name, input and output lane bindings, schema mode, validation flags, and streaming declaration. It does
not expose those lanes as composition outputs.

The online runner and delegated generated runner create one private implementation instance per hook-owning stage per
pipeline invocation. They dispatch hooks through that declaring class, pass only the source stage's declared hook
inputs, and apply existing hook validation at the original raw-hook boundary. A single source stage shares its one
instance across its hooks; separate stages never share one, even when they use the same transform class. Embedded
generated hooks retain the same stage/owner metadata and bindings but dispatch to the copied wrapper method instead.

`embed_hooks` is all-or-error for a composed generated artifact: every included raw hook must meet the existing
standalone embedding rules, including the separate `embed_udfs` requirement where applicable. The renderer must not
silently delegate one ineligible hook while embedding others, because that would make the caller's configured source
packaging preference unpredictable.

Traceability retains the existing stage-prefixed step identity and adds a hook boundary whose owner is the declaring
stage class. Explain output identifies the hook as stage-owned. Streaming compatibility evaluates every retained hook
in its source stage and reports the most restrictive finding for the composed pipeline.

## Rules

- Source-stage step methods and hooks run in source order within that stage.
- Cross-stage order follows `.to(...)` order or the dependency order induced by `stage(...)` output references.
- An independent `stage(...)` branch retains its local source order; no order is promised between independent branches
  unless a later stage consumes both outputs.
- Hooks execute on their declaring stage instance, not on the wrapper.
- Delegated generated `_impl` construction is per declaring stage, with stable imports and no shared mutable delegate.
- `embed_hooks` copies every eligible raw-hook body into the generated wrapper under a deterministic stage/owner name;
  it does not alter its placement, bindings, validation, traceability, or streaming classification.
- Traceability records the wrapper composition edge and the source-stage hook boundary.
- Streaming reporting is the minimum compatibility of every composed stage and hook.
- A hook-bearing stage is admitted only when generated and online execution have parity tests for hook order,
  validation boundaries, owner dispatch, traceability, and streaming reporting.

## Delivery Gate

Sprint 28 delivered this capability as a focused composition slice. The implementation removes only the
hook-bearing-stage rejection in both composition planners, carries owner-qualified hook plans through composition, and
adds matching online/generated execution delegates and embedded rendering. Acceptance evidence covers
dependency-respecting stage/hook order, one stage-local implementation object per delegated stage invocation, unchanged
hook schema validation, owner-qualified traceability, streaming diagnostics, and existing all-or-error embedded-hook
diagnostics. The Search label pipeline can consume this generic path without a Search-specific composition exception.

## Deferred Questions

Wrapper-local hooks, wrapper-local step methods, and cross-target hook pipelines remain deferred. Earlier-stage output
access is resolved by the public `TransformResult.stages` contract and the `allow_stage_outputs` configuration gate; it
does not expose existing lane internals.
