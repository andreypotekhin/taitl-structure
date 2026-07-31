"""Resolve the active Spark Geometry data type without naming a provider."""

from importlib.metadata import entry_points
from typing import Protocol


class GeometryProvider(Protocol):
    """Provider contract for Structure's common Spark SQL Geometry surface."""

    def geometry_type(self): ...


def geometry_type():
    """Return the active provider's Spark ``GEOMETRY`` data type.

    Providers register one ``structure.geo_provider`` entry point. Resolution is
    intentionally late so ordinary compilation and generated-code import remain
    Spark- and provider-free.
    """
    providers = tuple(entry_points(group="structure.geo_provider"))
    if not providers:
        raise RuntimeError(
            "GEO-E0901: The active Spark runtime has no Geometry SQL provider. "
            "Install a provider that implements Structure's GEOMETRY/ST_* contract."
        )
    if len(providers) != 1:
        names = ", ".join(sorted(provider.name for provider in providers))
        raise RuntimeError(
            f"GEO-E0902: Multiple Geometry SQL providers are installed: {names}. "
            "Keep exactly one active provider."
        )
    provider = providers[0].load()
    return provider.geometry_type()
