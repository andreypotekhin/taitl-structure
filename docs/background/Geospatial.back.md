# Geospatial

Spatial SQL looks more uniform than it is. A function named `ST_Intersects` says little about the physical Geometry
representation, the coordinate reference system (SRID), a Geography distinction, or whether an extension registered a
function that conflicts with a Spark builtin. Structure therefore treats a spatial value as a typed provider value, not
as an unqualified SQL object.

## Native Spark and providers

Native Spark adds Geometry, Geography, and a small WKB/SRID function set in PySpark 4.1. That is separate from the
default Structure PySpark profile, which ends before 4.1. Sedona and other providers have broader APIs, including WKT
constructors and predicates, but need their own dependencies, physical schemas, and evidence.

Native names stay familiar: `st_geomfromwkb`, `st_asbinary`, and `st_setsrid` are no-prefix names on an eligible native
target. An external provider uses a module such as `sedona.st_geomfromwkt`. The distinction makes it clear which
runtime owns an expression and makes provider APIs searchable from their source documentation.

## SRID and Binary

An SRID identifies the coordinate reference system used to interpret coordinates. Two equal-looking coordinates may not
be comparable if they use different reference systems. Structure tracks a fixed SRID when it is known statically and a
mixed SRID when it is not. It rejects unsafe comparisons rather than silently choosing a transformation.

Well-Known Binary (WKB) is a binary geometry representation. Standard WKB does not encode an SRID, so it is not a full
interchange record by itself. Extended WKB variants may carry SRID but remain provider conventions unless an exact
source/destination pair proves otherwise. A `binary()` field is consequently a deliberate application boundary, not a
portable Geometry declaration.

## Cell indexes

H3 and S2 model spatial cells, not general Geometry/Geography values. They are useful provider namespaces with their
own types and operations, but treating them as alternate implementations of generic ST functions would hide important
semantic differences. Structure keeps them as extensions.

For public behavior, see the [Geospatial reference](../reference/Geospatial.ref.md). For implementation policy, see the
[Geospatial design](../dev/design/Geospatial.design.md).
