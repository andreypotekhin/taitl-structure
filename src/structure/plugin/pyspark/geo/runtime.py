"""Resolve the active Spark Geometry data type without naming a provider."""

from importlib.metadata import entry_points
from typing import Any, Protocol, cast


class GeoProvider(Protocol):
    """Provider contract for Structure's common Spark SQL Geometry surface."""

    def geometry_type(self): ...

    def validate(self, operations: frozenset[str]) -> None: ...


def geometry_type():
    """Return the active provider's Spark ``GEOMETRY`` data type.

    Providers register one ``structure.geo_provider`` entry point. Resolution is
    intentionally late so ordinary compilation and generated-code import remain
    Spark- and provider-free.
    """
    providers = tuple(entry_points(group="structure.geo_provider"))
    if not providers:
        try:
            from structure.plugin.pyspark.geo.providers.sedona import provider as create_provider
        except ImportError as error:
            raise RuntimeError(
                "GEO-E0901: The active Spark runtime has no Geometry SQL provider. "
                "Install a provider that implements Structure's GEOMETRY/ST_* contract."
            ) from error
        return create_provider().geometry_type()
    if len(providers) != 1:
        names = ", ".join(sorted(provider.name for provider in providers))
        raise RuntimeError(
            f"GEO-E0902: Multiple Geometry SQL providers are installed: {names}. "
            "Keep exactly one active provider."
        )
    provider: Any = providers[0].load()
    if isinstance(provider, type) or callable(provider):
        provider = provider()
    return cast(GeoProvider, provider).geometry_type()
