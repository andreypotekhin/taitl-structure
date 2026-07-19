from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.platform.pyspark.dsl.joins.TiePolicy import TiePolicy


@dataclass(frozen=True)
class JoinDedupe:
    order_by: Any
    direction: str
    ties: TiePolicy = TiePolicy.ERROR

    @staticmethod
    def latest_by(order_by: object, *, ties: TiePolicy = TiePolicy.ERROR) -> JoinDedupe:
        return JoinDedupe._policy(order_by, direction="latest", ties=ties)

    @staticmethod
    def earliest_by(order_by: object, *, ties: TiePolicy = TiePolicy.ERROR) -> JoinDedupe:
        return JoinDedupe._policy(order_by, direction="earliest", ties=ties)

    @staticmethod
    def _policy(order_by: object, *, direction: str, ties: TiePolicy) -> JoinDedupe:
        if not hasattr(order_by, "kind") or not hasattr(order_by, "type"):
            raise TypeError(f"JoinDedupe.{direction}_by(order_by=...) requires a Structure expression")
        if order_by.kind == "order":
            raise TypeError(
                f"JoinDedupe.{direction}_by(order_by=...) requires an unordered expression; "
                f"{direction}_by(...) selects the ordering direction"
            )
        if getattr(order_by.type, "name", None) not in {
            "date", "decimal", "double", "float", "integer", "long", "string", "timestamp",
        }:
            raise TypeError(f"JoinDedupe.{direction}_by(order_by=...) requires an orderable scalar expression")
        if not isinstance(ties, TiePolicy):
            raise TypeError(f"JoinDedupe.{direction}_by(ties=...) requires a TiePolicy value")
        return JoinDedupe(order_by=order_by, direction=direction, ties=ties)
