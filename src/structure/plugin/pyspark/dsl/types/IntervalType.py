"""Spark interval types with exact ANSI field qualifiers."""

from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class IntervalType(StructureType):
    kind: str
    start_field: int | None
    end_field: int | None
    qualifier: str

    def __init__(self, kind: str, qualifier: str) -> None:
        year_month = ("year", "month")
        day_time = ("day", "hour", "minute", "second")
        if kind == "calendar":
            if qualifier != "calendar":
                raise ValueError("Calendar intervals have no ANSI field qualifier")
            fields: tuple[str, ...] = ()
            start = end = None
        else:
            fields = year_month if kind == "year_month" else day_time
            parts = qualifier.split("_to_")
            if not 1 <= len(parts) <= 2 or any(part not in fields for part in parts):
                raise ValueError(f"Unsupported {kind} interval qualifier: {qualifier!r}")
            start = fields.index(parts[0])
            end = fields.index(parts[-1])
            if start > end:
                raise ValueError(f"Interval qualifier fields are reversed: {qualifier!r}")
        object.__setattr__(self, "name", f"interval_{kind}")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "start_field", start)
        object.__setattr__(self, "end_field", end)
        object.__setattr__(self, "qualifier", qualifier)

    @property
    def sql(self) -> str:
        return "interval" if self.kind == "calendar" else f"interval {self.qualifier.replace('_to_', ' to ')}"
