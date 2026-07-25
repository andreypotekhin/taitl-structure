# Sprint 22: V5 Plugin Plugin Architecture Hardening

## Sprint Goal

Close the breaking migration, remove obsolete target coupling, and prove v5 ready for release.

## Product Outcome

Users and plugin authors have one coherent plugin model, PySpark behavior remains trustworthy, and no compatibility
shim obscures which package owns target APIs.

## Scope

### In Scope

- Preserve target-owned imports from their plugin packages—such as `structure.plugin.pyspark`—and verify the
  `structure` root remains target-neutral without changing end-user PySpark imports.
- Move legacy backend configuration, compatibility reporting, and PySpark-specific Core dispatch behind selected
  plugin facets. Structure retains those workflows as their orchestrator and public lifecycle owner.
- Upgrade, extension, capability, diagnostics, troubleshooting, and release documentation.
- Full regression, conformance, generated-artifact, supported-target, and performance-baseline evidence.
- Resolution or explicit deferral of v5 release blockers.

### Out of Scope

- New target plugins or transformation feature families.
- Cross-plugin pipelines, translation, or data interchange.
- Public support for private engine replacement.

## ExecPlan

`docs/dev/planning/P07162601.V5-plugin-architecture.plan.md`

## Acceptance Criteria

- No target-owned authoring name remains exported from the package root.
- No Core workflow imports a concrete PySpark plugin façade or service-facet implementation.
- PySpark and external-plugin conformance evidence pass against the released Plugin API range.
- Documentation consistently describes one target per transform and Core-owned workflows.
- Private replacement engines remain target-local, exact-revision-gated, and absent from public authoring guidance.
- `make build` and every required live target lane pass.

## Progress

- [x] (2026-07-23) Started after Sprint 21 completion. Confirmed that `structure` root exports are target-neutral and
  that PySpark imports remain correctly owned by `structure.plugin.pyspark`; began the configuration and Core-dispatch
  delegation audit.
- [x] (2026-07-23) Routed the configured default target and PySpark profile/variant through `plugin.default` and
  `plugin.pyspark` while retaining StructureConfig as the orchestrator-facing configuration boundary.
- [x] (2026-07-23) Routed unsupported-target compatibility guidance through plugin discovery. Core now lists installed
  targets and directs users to `plugin.default`, instead of embedding a PySpark-only backend recommendation.
- [x] (2026-07-23) Updated `structure init` to seed `plugin.default` and the PySpark-owned profile/variant table,
  so new projects start on the delegated configuration model.
- [x] (2026-07-23) Added the generic CLI `--target` override, which maps to the one-command `plugin.default`
  selection path without changing project configuration.
- [x] (2026-07-24) Added `StructureSession(target=...)` for session-local plugin selection.
- [x] (2026-07-24) Added `Capabilities.resolve()(target=...)` so Structure's compatibility workflow selects a plugin
  by the same generic target name while the plugin supplies its capability report.
- [x] (2026-07-24) Added `target=` to schema tooling, keeping the Core tool workflow while routing schema inspection
  and source rendering through the selected plugin's schema facet.
- [x] (2026-07-24) Removed the redundant PySpark-named Core project-renderer façade; the generic selected-plugin
  renderer remains the sole Core dispatch entry point.
- [x] (2026-07-24) Removed the legacy `target_backend=` session and capability overrides plus backend/profile/variant
  CLI flags. Public target selection now uses `target=` and `plugin.default`.
- [x] (2026-07-24) Replaced the end-user configuration guide's legacy target keys with `plugin.default` and the
  PySpark-owned profile/variant table; compatibility documentation asserts the new spelling.
- [x] (2026-07-24) Removed the Core configuration validator's implicit PySpark capability-model requirement. Core
  now invokes a target capability `require(...)` check only when that plugin supplies the corresponding surface.
- [x] (2026-07-24) Removed Core's `compat_targets` matrix and root-level target configuration keys. New defaults and
  accepted project overrides select `plugin.default`; PySpark profile and variant now live only in `plugin.pyspark`.
- [x] (2026-07-24) Changed the Plugin API capability facet to receive opaque selected-plugin options. Core now selects
  and invokes the capability facet without naming PySpark profile or variant semantics.
- [x] (2026-07-24) Removed PySpark profile and variant fields from `CompilerOptions`; compilation now forwards the
  selected plugin's opaque table, and the PySpark compiler reads its own settings from `plugin_options`.
- [x] (2026-07-24) Removed profile and variant fields from `StructureConfig` and exposed `StructureSession.target`
  plus `plugin_options`; PySpark runtime code now reads those target-specific settings only from its option table.
- [x] (2026-07-24) Renamed Core configuration and compiler-routing fields from `target_backend` to `target`.
  Target selection is now consistently generic through config, session, artifact, CLI, docs, and generation workflows.
- [x] (2026-07-24) Removed `target_variant` from the schema-inspection Plugin API request. Schema tooling now passes
  opaque selected-plugin options, and PySpark interprets its own variant setting in the plugin schema facet.
- [x] (2026-07-24) Made `RuntimeDiagnostic` generic with a `target` field. PySpark profile and variant are now
  plugin-added diagnostic context rather than Core runtime-diagnostic fields.
