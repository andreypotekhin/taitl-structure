"""A finite iterable starter plugin for Structure plugin authors."""

from .dsl import field, inner_join, left_join, scan, state
from .IterablePlugin import IterablePlugin

__all__ = ["IterablePlugin", "field", "inner_join", "left_join", "scan", "state"]
