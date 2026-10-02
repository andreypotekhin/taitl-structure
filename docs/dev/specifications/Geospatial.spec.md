# Geospatial Specification

## Status

This is a target-gated specification. The default PySpark `>=3.5,<4.1` profile implements none of the interfaces below.
The current experimental Geometry symbols are not a stable compatibility contract.

## Native API

On a certified native PySpark 4.1+ target, Structure will export these no-prefix helpers:

```python
st_geomfromwkb(value, srid=None)
st_geogfromwkb(value)
st_asbinary(value)
st_srid(value)
st_setsrid(value, srid)
```

`value` is a typed Binary expression for constructors and a native Geometry or Geography expression for consumers.
Nullability follows the input. `st_srid` returns nullable Integer. Geometry construction without `srid` produces fixed
SRID 0; a literal SRID produces that fixed SRID; a typed integral expression produces mixed SRID. Geography construction
produces fixed SRID 4326. `st_setsrid` preserves kind and dialect, with the same fixed-versus-mixed SRID rule.

## External provider API

An external provider is exported as a module, for example `sedona`. Its public helpers use the provider's documented
lowercase `st_*` spelling, for example:

```python
sedona.st_geomfromwkt(value, srid=None)
sedona.st_astext(value)
sedona.st_intersects(left, right)
sedona.st_contains(left, right)
sedona.st_within(left, right)
sedona.st_asewkb(value)
sedona.st_geomfromewkb(value)
```

The module appears only as a target-gated API until its adapter proves each signature. An omitted or nonliteral SRID
returns a mixed-SRID value. A literal SRID returns a fixed-SRID value. Provider helpers require an effective matching
`geo_provider`; mismatches are compiler diagnostics, not runtime casts.

## Provider scope

Future configuration and decorators use these forms:

```toml
[tool.structure.plugin.pyspark]
geo_provider = "sedona"
```

```python
@transform(geo_provider="sedona")
class Boundaries(Transform):
    @step(input=source, output=result, geo_provider="sedona")
    def parse(self, row): ...
```

`@step` wins over `@transform`, which wins over application configuration. With no value, Structure infers `native`
only on a certified native target. Otherwise a spatial declaration or helper reports that no compatible provider exists.

## Compatibility and conversion

Spatial predicates and other operations that require comparable values accept only values of one dialect, one kind, and
one fixed SRID. They reject mixed SRID, cross-kind, and cross-provider operands. A direct value cannot cross a provider
scope.

An ordinary `binary()` field may cross scopes. It carries no codec, SRID, kind, or compatibility proof. WKB requires a
separate SRID when that fact matters. EWKB and any other extended encoding are caller-owned; a provider pair may be
promoted only after its decoder and preservation behavior are tested explicitly.

## Provider admission

A provider adapter must supply a collision-safe renderer, physical type materializer, capability matrix, dependency
check, and public diagnostics. It must prove classic execution, generated/online parity, and every claimed target mode.
Spark Connect and streaming are unsupported until separately proven. H3 and S2 adapters are namespaced cell-index APIs
and cannot be selected as Geometry/Geography providers.
