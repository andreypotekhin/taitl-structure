from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.ScalarType import ScalarType


@dataclass(frozen=True)
class SketchType(ScalarType):
    """Opaque Spark binary state branded with its sketch family and profile."""

    algorithm: str
    profile: str = "baseline"

    def __init__(self, algorithm: str, profile: str = "baseline") -> None:
        if not algorithm or not profile:
            raise ValueError("Sketch algorithm and profile must be non-empty")
        object.__setattr__(self, "name", "sketch")
        object.__setattr__(self, "algorithm", algorithm)
        object.__setattr__(self, "profile", profile)


class HllSketchType(SketchType):
    lg_config_k: int

    def __init__(self, profile: str = "baseline", lg_config_k: int = 12) -> None:
        if isinstance(lg_config_k, bool) or not isinstance(lg_config_k, int) or not 4 <= lg_config_k <= 21:
            raise ValueError("HLL lg_config_k must be an integer from 4 through 21")
        super().__init__("hll", profile)
        object.__setattr__(self, "lg_config_k", lg_config_k)


class BitmapType(SketchType):
    def __init__(self, profile: str = "baseline") -> None:
        super().__init__("bitmap", profile)


class KllSketchType(SketchType):
    def __init__(self, profile: str = "pyspark4.1") -> None:
        super().__init__("kll", profile)


class ThetaSketchType(SketchType):
    def __init__(self, profile: str = "pyspark4.1") -> None:
        super().__init__("theta", profile)
