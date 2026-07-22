"""Shared evaluation-batch contracts."""

from structure import Schema
from structure.plugin.pyspark import TimeWindow, struct


class EvaluationBatch(Schema):
    """One UTC-aligned daily window selected by the caller."""

    window = struct(TimeWindow, nullable=False)
