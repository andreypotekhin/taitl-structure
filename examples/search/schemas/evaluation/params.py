"""Evaluation slice parameters."""

from examples.search.schemas.label import Label
from structure import Schema
from structure.plugin.pyspark import *


class EvaluationParams(Schema):
    """One caller-defined label and optional band evaluation slice."""

    labels = array(struct(Label), contains_null=False, nullable=False)
    band_id = string(nullable=True)
