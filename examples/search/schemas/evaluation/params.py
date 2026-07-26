"""Evaluation slice parameters."""

from examples.search.schemas.label import Label
from structure import Schema
from structure.plugin.pyspark import *


class EvaluationParams(Schema):
    """One caller-defined evaluation slice; future bands extend this contract."""

    labels = array(struct(Label), contains_null=False, nullable=False)
