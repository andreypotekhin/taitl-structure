# Geospatial Background

Spatial support has two distinct identities. The helper namespace chooses a Python implementation; the declared field
type chooses the physical value representation. Root helpers target Apache PySpark, `databricks.*` targets the
Databricks Python namespace, and `sedona.*` targets Sedona. No application setting or scope precedence selects a
provider.

Native Spark Geometry and Geography are shared schema types when the active runtime exposes compatible constructors.
Databricks can backport Spark APIs, so type-constructor availability is checked directly rather than inferred only
from the Spark version. Sedona Geometry remains a separate UDT, and its logical SRID is recorded in schema metadata.

Binary encoders are necessary for crossing representations, but are not currently sufficient in one Spark session.
Spark 4.1 live tests show Sedona's SQL extension replaces Spark's same-named `st_*` registrations, so native and
Sedona workloads must use separate sessions. WKB drops SRID; the caller must carry that information independently.
A similarly named helper does not establish conversion compatibility or collision-free registration.

The default profile is `>=3.5,<4.2`. Apache spatial helpers are scoped to exact PySpark 4.1; Databricks and Sedona
have independent requirements. A managed runtime and Databricks Connect each need live evidence. See the
[Geospatial reference](../reference/Geospatial.ref.md) for current API usage and support boundaries.
