from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.plugin.pyspark.dsl.joins.TiePolicy import TiePolicy
from structure.plugin.pyspark.dsl.options import tie_policy


@dataclass(frozen=True)
class JoinDedupe:
    order_by: Any
    direction: str
    ties: TiePolicy = TiePolicy.ERROR

    @staticmethod
    def latest_by(order_by: object, *, ties: TiePolicy | str = TiePolicy.ERROR) -> JoinDedupe:
        return JoinDedupe._policy(order_by, direction="latest", ties=ties)

    @staticmethod
    def earliest_by(order_by: object, *, ties: TiePolicy | str = TiePolicy.ERROR) -> JoinDedupe:
        return JoinDedupe._policy(order_by, direction="earliest", ties=ties)

    @staticmethod
    def _policy(order_by: object, *, direction: str, ties: TiePolicy | str) -> JoinDedupe:
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
        return JoinDedupe(
            order_by=order_by,
            direction=direction,
            ties=tie_policy(ties, call=f"JoinDedupe.{direction}_by(...)"),
        )
