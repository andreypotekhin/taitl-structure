"""A finite iterable starter plugin for Structure plugin authors."""

from .dsl import grouped, inner_join, left_join, projection, recurrence, state
from .IterablePlugin import IterablePlugin

__all__ = ["IterablePlugin", "grouped", "inner_join", "left_join", "projection", "recurrence", "state"]
