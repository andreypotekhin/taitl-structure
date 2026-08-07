# Retrospects

This page records release-level lessons. It is historical evidence, not a replacement for the living implementation
plans or their detailed outcomes.

## v3: PySpark Parity and Streaming-Transformation Hardening

### Highlights

- The release closed its declared parity slices in a coherent order: scalar DSL and SQL helpers, joins, aggregation,
  windows, collections, then streaming-transformation hardening. The completed Sprint 11–16 plans retain the decisions,
  edge cases, and test outcomes for each slice.
- The shared PySpark recipe path remained the release’s strongest architectural choice. New operations were captured
  symbolically, capability checked, rendered as readable generated PySpark, and executed online through the same plan.
  This prevented the two execution modes from becoming separate products.
- v3 made safety rules explicit instead of approximating PySpark permissiveness: lookup results stay nullable, PySpark
  4-only helpers are capability gated, selected joins and windows require deterministic contracts, and unsupported
  shapes fail before a Spark action when static information is sufficient.
- The release clarified the streaming boundary. Structure transforms caller-provided streaming DataFrames; callers own
  sources, sinks, triggers, checkpoints, output modes, and query lifecycle. The streams example and its generated
  artifact make that boundary concrete.
- The project added representative live concept parity coverage alongside detailed unit and specification tests. This
  is the right release-level proof for an IR compiler: the same public transform must agree online and as generated
  code on each claimed target.

### Lowlights

- v3’s parity work was initially tracked as separate gap lists. That was effective for delivery, but it did not give a
  developer one authoritative answer for every relevant PySpark transformation API. The absent coverage catalog is the
  primary v4 foundation gap.
- Live evidence was gathered incrementally and focused runs exposed target-specific capability exclusions. The full
  integration matrix still needs a clean baseline: the current concept-and-release-evidence plan records a pre-existing
  failure in `tests/integration/pyspark/v1/test_execution_parity.py` during unfiltered classic integration runs.
- Several advanced capabilities were correctly left outside v3—row generators, nearest as-of joins, richer parsers,
  and chained streaming state—but their deferral makes the release boundary harder to discover from a simple API list.
  V4 must make every such decision visible in one catalog.
- Completion evidence exists in the individual sprint plans, but release evidence should be recorded once, with exact
  target versions, commands, pass/skip totals, and explicit deferrals. The v4 hardening sprint now makes that a
  release requirement.

### Lessons Carried Forward

- Admit an operation only with its complete contract: types, nullability, cardinality, target capability,
  streaming classification, diagnostics, generated rendering, and online/generated parity.
- Prefer an honest deferral or an explicit `@raw` boundary to a partial dynamic wrapper.
- Make target-specific support visible before a user reaches a Spark runtime.
- Treat a passing Spark-free build as necessary baseline evidence, not as proof of live PySpark compatibility.

## v4 Readiness Review

Review date: 2026-07-15.

V4 is ready to begin its foundation sprint, but it is not ready to claim release readiness. Its product boundary,
delivery order, streaming slice, final hardening charter, and acceptance standard are defined in the historical
`docs/dev/design/V4TransformationApiCoverage.design.md`, the Sprint 17 and Sprint 18 charters, and the v4 ExecPlans. The
remaining gates below are intentionally release-blocking.

### Ready

- The release is bounded to transformations over caller-supplied DataFrames. Loading, writes, actions, catalog work,
  streaming lifecycle ownership, non-batch Spark Connect, and alternative backends are expressly outside v4.
- The v4 catalog has clear admission rules: supported operations must remain typed, symbolic, capability checked,
  explainable, and equivalent in online and generated execution.
- The release sequence respects dependencies: coverage foundation first; scalar, nested, relational, streaming, and
  generator work next; hardening only after feature work ends.
- The caller-owned streaming migration has a design, a specification, a scoped Sprint 18 charter, and an ExecPlan.
  Its state and output-mode restrictions are conservative and testable.
- The final hardening sprint requires default-build, classic-target, applicable Connect, generated-artifact,
  documentation, diagnostics, and performance evidence without expanding scope.

### Gaps Closed by This Review

- `docs/dev/Gaps.md` now defines every status it currently uses and distinguishes pre-catalog `planned` work from
  catalog `scheduled` work. A `deferred` item now has an explicit meaning rather than looking like untracked work.
- This release-level record now connects v3 lessons to v4 admission and release gates, so future work can distinguish
  a completed v3 scope from an unfinished PySpark coverage program.

### Remaining Gates Before V4 Can Be Released

1. Sprint 17 must create the checked local PySpark 3.5.x/4.0.x inventory, the public coverage catalog, its integrity
   tests, and the v4 fixture. No catalog file exists yet, so the release cannot yet demonstrate its coverage promise.
2. Each catalog entry must be classified as supported, scheduled, deferred, or unsupported and must give a Structure
   spelling, an alternative, or a specific rationale. Existing `Gaps.md` tables are planning input, not the catalog.
3. Every supported v4 family needs target-specific capability and live online/generated parity evidence. The current
   classic integration matrix is not yet a release-evidence baseline. During this review, `make integration
   BACKEND=pyspark35` reached its 19 tests and failed once in `tests/integration/pyspark/v1/test_execution_parity.py`;
   the isolated embedded-hook parity test then passed. `make integration BACKEND=pyspark40` failed before test
   collection while Compose recreated containers (`No such container`). Reproduce each cleanly, fix it or record an
   explicit deferral through the normal issue process before the final hardening sprint treats either lane as evidence.
4. The generator gate must produce a schema-and-cardinality design decision before any `explode`, `posexplode`, or
   `inline` helper is admitted.
5. The final hardening ExecPlan must record exact commands, backend versions, passed/skipped totals, and links to
   accepted deferrals after all feature sprints are complete. Its sprint charter exists; its evidence remains future
   work.

### v4 Entry Decision

Proceed with Sprint 17. Do not mark M9 complete, advertise broad transformation coverage, or cut a v4 release until
all five remaining gates have observable evidence.
