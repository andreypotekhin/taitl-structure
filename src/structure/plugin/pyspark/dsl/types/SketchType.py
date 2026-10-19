from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.ScalarType import ScalarType


@dataclass(frozen=True)
class SketchType(ScalarType):
    """Opaque Spark binary state branded with its logical sketch family."""

    algorithm: str

    def __init__(self, algorithm: str) -> None:
        if not algorithm:
            raise ValueError("Sketch algorithm must be non-empty")
        object.__setattr__(self, "name", "sketch")
        object.__setattr__(self, "algorithm", algorithm)


@dataclass(frozen=True)
class HllSketchType(SketchType):
    lg_config_k: int

    def __init__(self, lg_config_k: int = 12) -> None:
        if isinstance(lg_config_k, bool) or not isinstance(lg_config_k, int) or not 4 <= lg_config_k <= 21:
            raise ValueError("HLL lg_config_k must be an integer from 4 through 21")
        super().__init__("hll")
        object.__setattr__(self, "lg_config_k", lg_config_k)


class BitmapType(SketchType):
    def __init__(self) -> None:
        super().__init__("bitmap")


@dataclass(frozen=True)
class KllSketchType(SketchType):
    domain: str

    def __init__(self, domain) -> None:
        domain = getattr(domain, "type", domain)
        name = getattr(domain, "name", None)
        if name not in {"long", "float", "double"}:
            raise TypeError("kll_sketch(...) domain must be long(), float(), or double()")
        super().__init__("kll")
        object.__setattr__(self, "domain", name)


class ThetaSketchType(SketchType):
    def __init__(self) -> None:
        super().__init__("theta")
