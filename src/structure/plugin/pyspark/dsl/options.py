from __future__ import annotations

from enum import Enum
from typing import TypeVar

from structure.plugin.pyspark.dsl.joins.AsOf import AsOf
from structure.plugin.pyspark.dsl.joins.Join import Join
from structure.plugin.pyspark.dsl.joins.JoinHint import JoinHint
from structure.plugin.pyspark.dsl.joins.JoinStrategy import JoinStrategy
from structure.plugin.pyspark.dsl.joins.OverlapPolicy import OverlapPolicy
from structure.plugin.pyspark.dsl.joins.TiePolicy import TiePolicy

E = TypeVar("E", bound=Enum)


def join(value: Join | str, *, call: str) -> Join:
    return _option(Join, value, call=call, name="how")


def as_of(value: AsOf | str, *, call: str) -> AsOf:
    return _option(AsOf, value, call=call, name="direction")


def join_hint(value: JoinHint | str | None, *, call: str) -> JoinHint | None:
    if value is None:
        return None
    return _option(JoinHint, value, call=call, name="hint")


def join_strategy(value: JoinStrategy | str | None, *, call: str) -> JoinStrategy | None:
    if value is None:
        return None
    if value == "broadcast":
        return JoinStrategy.BROADCAST_HASH
    if value == "merge":
        return JoinStrategy.SORT_MERGE
    return _option(JoinStrategy, value, call=call, name="strategy")


def overlap_policy(value: OverlapPolicy | str, *, call: str) -> OverlapPolicy:
    return _option(OverlapPolicy, value, call=call, name="overlaps")


def tie_policy(value: TiePolicy | str, *, call: str) -> TiePolicy:
    return _option(TiePolicy, value, call=call, name="ties")


def _option(type_: type[E], value: E | str, *, call: str, name: str) -> E:
    if isinstance(value, type_):
        return value
    if isinstance(value, str):
        for option in type_:
            if value == option.value:
                return option
    expected = ", ".join(f'"{option.value}"' for option in type_)
    raise TypeError(f"{call} {name}= must be one of {expected}")
