# Plugin Configuration

## Purpose

This specification defines the released v5 configuration and selection contract for installed plugins. It replaces
the pre-v5 `target_backend`, `target_profile`, and `target_variant` settings; legacy keys are rejected with an
actionable configuration diagnostic and are never silently translated.

Core owns plugin selection and configuration resolution. A selected plugin receives only its own opaque
configuration mapping; it cannot read another plugin's settings or alter Core's selection rules.

## Configuration Shape

The following TOML is the complete v5 plugin configuration shape. The same keys are valid in `structure.toml` and
under `[tool.structure]` in `pyproject.toml`.

```toml
[tool.structure.plugin]
default = "pyspark"
disabled_distributions = ["example-old-pyspark"]
plugin_options = "allow_injection"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

`plugin.default` is optional. It selects a plugin only for a transform that has neither
`@transform(target=...)` nor an explicit caller target. It is a plugin name, never a distribution name.
Plugin names use the exact lowercase syntax defined in [PluginAPI.md](PluginAPI.md); they are not normalized or
treated as distribution names.

`plugin.disabled_distributions` is an ordered-free set of normalized Python distribution names. Core compares names
using the packaging normalization rule: case-insensitively, with runs of `.`, `_`, and `-` treated alike. A disabled
distribution is ignored during discovery and must not have its entry point loaded.

`plugin.plugin_options` is an optional global Structure setting. It is absent by default, which forbids every plugin
from replacing a private Core engine class. Its sole v5 value is `"allow_injection"`; that value permits class injection
for the selected plugin only after its private manifest passes the separate Structure-version and engine-revision checks.
The setting is deliberately global rather than target-specific. Users must enable it only when they trust every
installed and eligible plugin that might be selected for the current run.

`plugin.<name>` is an opaque TOML table for the named plugin. Core validates that it is a table, copies it as
immutable data, and passes it only after that named plugin has been selected and negotiated. Apart from its TOML type,
key spelling and value meaning belong to the plugin. Core must neither reserve plugin-table keys nor merge the table
with another plugin's table.

The bundled PySpark plugin owns `plugin.pyspark.profile` and `plugin.pyspark.variant`. Their v5 meanings are the
same as the prior PySpark profile and variant settings. This preserves the released PySpark target range without
making it a Core-wide backend setting.

## Precedence and Validation

Plugin settings use the normal configuration precedence, lowest to highest: built-in defaults, `structure.toml`,
`pyproject.toml [tool.structure]`, then CLI or Python API overrides. A higher-precedence `plugin.<name>` table merges
by key with the lower-precedence table of the same name. Lists replace rather than append; in particular, an override
of `disabled_distributions` replaces the full list.

The Python override spelling mirrors the TOML structure. For example, callers may use
`overrides={"plugin.default": "pyspark", "plugin.pyspark.profile": ">=4.0,<4.1"}`. Core validates the final,
merged configuration before plugin implementation code is loaded. It must reject an empty plugin name, a non-string
default, a non-list disablement value, non-string distribution names, and a non-table `plugin.<name>` value.
It must also reject a non-string `plugin_options` value or a string other than `"allow_injection"`.

Configuration validation does not require a plugin to be installed. A configured default is resolved only when a
transform needs it. This keeps compiler configuration inspection Spark-free and allows a project to carry settings for
an optional plugin.

## Geo Providers

The PySpark Geometry API is provider-neutral and needs no provider configuration. Generated code calls the common
Spark SQL `GEOMETRY`/`ST_*` contract without provider imports. Spark-free compilation and code generation remain
available; only schema materialization and online execution verify that the active runtime supplies that contract.

## Target Resolution

Resolve one transform target in this exact order:

1. Read the transform's optional `@transform(target=...)` value.
2. If the caller supplied an explicit target, require it to equal the decorator value when both exist; otherwise use it.
3. If neither source selected a target, use `plugin.default`.
4. If no target is available, fail before plugin loading.
5. Discover eligible metadata for the resolved name, reject zero or multiple eligible plugins, then load and
   negotiate the unique plugin.
6. If that plugin declares private engine replacement, require `plugin.plugin_options = "allow_injection"` before
   resolving or constructing any replacement class. Otherwise fail the requested transform with a `PLUGIN` diagnostic
   that names the plugin and says class injection is disabled by default. Its remedy must show the exact opt-in setting
   and advise enabling it only for trusted plugins.

Project compilation applies this algorithm independently to every transform. Composition resolves every stage first,
then requires all resolved plugin names to be identical before invoking a schema or compiler facet. A raw constraint
can narrow the selected plugin's profile or variant only; it cannot select or replace the plugin.

The explicit target is a per-invocation value. It does not mutate `plugin.default`, configuration files, or process
state. A `StructureSession` may cache immutable discovery metadata and negotiated façades for its own lifetime, but it
must not create a globally active plugin.

## Diagnostics and Acceptance Evidence

v5 reserves the `PLUGIN` diagnostic prefix in [Diagnostics.md](Diagnostics.md). Plugin-configuration validation,
discovery, resolution, negotiation, and lifecycle-facet consistency use the named `PLUGIN` diagnostics. Every
target-resolution error must identify the transform, requested plugin name when known, and the shortest safe remedy.

Tests must cover table merging, list replacement, normalized disablement, the complete resolution order, a decorator
and explicit-target conflict, missing default target, cross-target composition rejection before plugin loading, and
the guarantee that a disabled plugin module never enters `sys.modules`.
They must also cover the default injection rejection, the exact `"allow_injection"` opt-in, invalid option values, and
the guarantee that a blocked plugin's replacement classes are never resolved or constructed.
