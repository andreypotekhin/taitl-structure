# Design: Geospatial Provider Boundaries

## Purpose

Structure must make a spatial value's physical implementation as explicit as its SQL type. Native Spark Geometry and
Geography, Sedona Geometry, and future Mosaic-like providers do not become interchangeable because their functions have
similar names. This design provides a migration-friendly native surface and an equal, explicit place for provider APIs
without pretending their values, codecs, or runtime setup are portable.

The delivery and evidence sequence is [P10012602](../planning/P10012602.Geospatial-provider-boundaries.plan.md). This
document defines future behavior; it does not promote geospatial support in the PySpark `>=3.5,<4.1` baseline.

## Namespaces

Native PySpark 4.1+ uses no provider prefix. Its Structure helpers retain the public PySpark names:

| Native helper | Logical result or rule |
| --- | --- |
| `st_geomfromwkb(wkb, srid=None)` | Geometry; omitted SRID is fixed 0, literal SRID is fixed, symbolic SRID is mixed |
| `st_geogfromwkb(wkb)` | Geography with fixed SRID 4326 |
| `st_asbinary(geo)` | Binary WKB; no portable SRID claim |
| `st_srid(geo)` | nullable Integer SRID |
| `st_setsrid(geo, srid)` | same kind; literal SRID is fixed and symbolic SRID is mixed |

There is intentionally no `native_*` namespace. Native names are available only when the target profile supports them.

Every external provider uses a namespace and keeps names easy to search from its native documentation:

```python
from structure.plugin.pyspark import sedona

shape = sedona.st_geomfromwkt(row.wkt, 4326)
inside = sedona.st_intersects(shape, area)
payload = sedona.st_asewkb(shape)
```

Mosaic and future Geometry providers follow the same pattern. H3 and S2 remain namespaced cell-index extensions; they
are not implementations of the Geometry/Geography backend contract.

## Provider selection and values

`geo_provider` is selected at application configuration, `@transform`, or `@step`. The effective value resolves in this
order: step, transform, application, then native inference. Native inference occurs only when the resolved PySpark
target supports native spatial types. An external namespace requires the same effective provider.

Provider selection does more than name a renderer. It fixes the physical Spark schema materialization, validates
runtime requirements, and prevents a provider operation from accidentally consuming another provider's spatial value.
External provider renderers must use their supported Python API or an unambiguous provider function; they may not rely
on registering an unqualified `ST_*` name that happens to win Spark function resolution.

Every spatial type records three facts:

- dialect: `native` or an external provider identifier;
- kind: Geometry or Geography; and
- SRID: a fixed integer or mixed (`None`).

`geometry(srid=...)` and `geography(srid=...)` are declaration forms resolved against the effective provider. A fixed
field describes one SRID. `srid=None` describes mixed values. A provider constructor with an omitted or nonliteral SRID
produces mixed SRID rather than claiming a provider default as static knowledge.

Operations requiring spatial comparability require one dialect, one kind, and the same fixed SRID. Mixed-SRID and
cross-kind predicate operands fail before Spark execution. A later design may add explicit runtime validation; it does
not exist in this contract.

## Binary handoff

Spatial values never pass directly from one provider scope to another. A caller may deliberately project one provider's
representation into a declared ordinary `binary()` field and consume it in a later provider-scoped step. Structure
recognizes that Binary is no longer a spatial value and does not infer its codec.

Standard WKB does not carry SRID. A caller that uses WKB and needs SRID must preserve it separately and restore it in
the receiving provider. Provider formats such as Sedona EWKB may embed SRID, but no Structure API promises that a
different provider decodes those bytes or preserves dimensions, kind, or CRS. Codec validation is application-owned
until a source/destination provider pair has dedicated evidence.

## Capability and evidence

A future provider adapter declares its identifier, physical Geometry/Geography schema materializers, supported helper
signatures, supported profile/variant modes, renderer, and runtime prerequisites. Every helper is admitted separately:
similar SQL names do not imply shared null, SRID, geography, or malformed-input semantics.

Native ST support requires PySpark 4.1+ capability entries. Each external provider requires its own profile and
dependency entries. A promotion requires symbolic typing, generated rendering, online/generated parity, positive
ordinary-runtime evidence, Spark Connect evidence before claiming Connect, and streaming evidence before claiming
streaming. Collision tests must run native and the provider together when the target permits both.

## Non-goals

This design does not provide coordinate transformation, measurements, spatial indexes or joins, automatic provider
conversion, portable persistence, arbitrary provider SQL, or H3/S2 Geometry emulation. Those capabilities need their
own typed inputs, outputs, cost/cardinality, and target contracts.
