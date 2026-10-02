# Geospatial reference

Geospatial support is target-gated. The default PySpark `>=3.5,<4.1` target does not provide a stable Geometry,
Geography, or provider API. This page explains the planned contract and how to keep provider-specific code explicit
until its target evidence is complete.

## Native Spark

Native PySpark 4.1+ is the planned home for no-prefix `st_*` helpers:

| Helper | Purpose |
| --- | --- |
| `st_geomfromwkb(value, srid=None)` | Create native Geometry from Binary WKB |
| `st_geogfromwkb(value)` | Create native Geography from Binary WKB |
| `st_asbinary(value)` | Produce Binary WKB |
| `st_srid(value)` | Read the SRID |
| `st_setsrid(value, srid)` | Return a value with the supplied SRID |

These helpers are unavailable on the default baseline. Do not use a `native_*` prefix: native Spark owns the root names.

## External providers

External provider functions live in provider modules and retain their documented `st_*` names:

```python
from structure.plugin.pyspark import sedona

shape = sedona.st_geomfromwkt(row.wkt, 4326)
matched = sedona.st_intersects(shape, region)
```

Provider APIs require a matching `geo_provider` selection at application, transform, or step level. Step settings win
over transform settings, which win over application settings. Provider modules remain target-gated until Structure has
certified their dependency, physical type, renderer, and execution modes.

## Types and SRIDs

`geometry(srid=...)` and `geography(srid=...)` will bind to the effective provider. A literal SRID declares a fixed
coordinate reference system. `srid=None`, an omitted provider SRID, or a symbolic SRID is mixed. Operations such as
spatial predicates require the same provider, kind, and fixed SRID. Structure does not transform coordinates or perform
runtime compatibility checks automatically.

## Provider handoffs

Pass a spatial value between provider-scoped steps only by projecting an ordinary `binary()` field. Binary has no
declared codec or portability guarantee. Standard WKB does not preserve SRID; extended formats such as EWKB may be
useful for a particular integration, but the application owns the receiving decoder and its compatibility tests.

See [Geospatial provider bridge](../recipes/GeospatialProviderBridge.md) for the explicit boundary pattern and
[Configuration](ConfigSchema.ref.md) and [Transform](Transform.ref.md) for future scope placement.
