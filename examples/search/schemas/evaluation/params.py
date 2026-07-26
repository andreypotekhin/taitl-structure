"""Evaluation slice parameters."""

from examples.search.schemas.label import Label
from examples.search.schemas.user import Cohort
from structure import Schema
from structure.plugin.pyspark import *


class EvaluationParams(Schema):
    """One caller-defined label and optional user-band evaluation slice."""

    labels = array(struct(Label), contains_null=False, nullable=False)
    user_band = struct(Cohort, nullable=True)
