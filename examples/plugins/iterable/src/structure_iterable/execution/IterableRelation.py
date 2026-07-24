from collections.abc import Iterable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class IterableRelation:
    """Materialized finite rows with repeatable collection for the fixture runtime."""

    rows: tuple[dict[str, object], ...]

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, object]]) -> "IterableRelation":
        return cls(tuple(dict(row) for row in rows))

    def collect(self) -> list[dict[str, object]]:
        return [dict(row) for row in self.rows]
