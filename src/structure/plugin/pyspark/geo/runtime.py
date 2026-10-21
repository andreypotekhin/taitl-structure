"""Lazy dispatch for explicitly selected spatial Python APIs."""

from importlib import import_module


def call_geospatial_function(provider: str, operation: str, *arguments):
    """Call exactly the provider named in the compiled expression."""
    modules = {
        "apache": ("pyspark.sql.functions", operation),
        "databricks": ("pyspark.databricks.sql.functions", operation),
    }
    if provider == "sedona":
        module_name, function_name = _sedona_function(operation)
    else:
        selected = modules.get(provider)
        if selected is None:
            raise ValueError(f"Unknown geospatial helper namespace {provider!r}")
        module_name, function_name = selected
    try:
        module = import_module(module_name)
        function = getattr(module, function_name)
    except (ImportError, AttributeError) as error:
        raise RuntimeError(
            f"The {provider}.{operation}(...) helper is unavailable. Install the matching provider "
            "Python and runtime packages; see docs/reference/Geospatial.ref.md."
        ) from error
    return function(*arguments)


def _sedona_function(operation: str) -> tuple[str, str]:
    wrappers = {
        "st_geomfromwkt": ("sedona.spark.sql.st_constructors", "ST_GeomFromWKT"),
        "st_geomfromwkb": ("sedona.spark.sql.st_constructors", "ST_GeomFromWKB"),
        "st_geomfromewkb": ("sedona.spark.sql.st_constructors", "ST_GeomFromEWKB"),
        "st_astext": ("sedona.spark.sql.st_functions", "ST_AsText"),
        "st_asbinary": ("sedona.spark.sql.st_functions", "ST_AsBinary"),
        "st_asewkb": ("sedona.spark.sql.st_functions", "ST_AsEWKB"),
        "st_srid": ("sedona.spark.sql.st_functions", "ST_SRID"),
        "st_setsrid": ("sedona.spark.sql.st_functions", "ST_SetSRID"),
        "st_intersects": ("sedona.spark.sql.st_predicates", "ST_Intersects"),
        "st_contains": ("sedona.spark.sql.st_predicates", "ST_Contains"),
        "st_within": ("sedona.spark.sql.st_predicates", "ST_Within"),
    }
    try:
        return wrappers[operation]
    except KeyError as error:
        raise ValueError(f"Unknown Sedona helper {operation!r}") from error
