"""User-facing option parsing for PySpark join helpers.

Join helpers accept either the exported enum values or their documented string
values.  These functions keep that convenience consistent across the DSL and
produce errors that name the original helper call and option.
"""

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
    """Accept a join direction for helpers such as ``lookup_join``.

    Args:
        value: ``Join`` enum value or one of its string values.
        call: Helper call text used in validation messages.

    Returns:
        The selected ``Join`` enum value.
    """
    return _option(Join, value, call=call, name="how")


def as_of(value: AsOf | str, *, call: str) -> AsOf:
    """Accept the time search direction for ``as_of_one`` joins."""
    return _option(AsOf, value, call=call, name="direction")


def join_hint(value: JoinHint | str | None, *, call: str) -> JoinHint | None:
    """Accept an optional Spark join hint, such as broadcast."""
    if value is None:
        return None
    return _option(JoinHint, value, call=call, name="hint")


def join_strategy(value: JoinStrategy | str | None, *, call: str) -> JoinStrategy | None:
    """Accept an optional physical Spark join strategy.

    The short aliases ``"broadcast"`` and ``"merge"`` map to the broadcast-hash
    and sort-merge strategies respectively.
    """
    if value is None:
        return None
    if value == "broadcast":
        return JoinStrategy.BROADCAST_HASH
    if value == "merge":
        return JoinStrategy.SORT_MERGE
    return _option(JoinStrategy, value, call=call, name="strategy")


def overlap_policy(value: OverlapPolicy | str, *, call: str) -> OverlapPolicy:
    """Accept the policy for overlapping temporal validity ranges."""
    return _option(OverlapPolicy, value, call=call, name="overlaps")


def tie_policy(value: TiePolicy | str, *, call: str) -> TiePolicy:
    """Accept the policy for duplicate best rows in ordered selections."""
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
