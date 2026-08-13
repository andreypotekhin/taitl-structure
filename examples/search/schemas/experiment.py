"""Experiment metadata for Search score variants."""

from structure import Schema
from structure.plugin.pyspark import *


class Experiment(Schema):
    """A single run of a named experiment."""

    experiment_id = string(nullable=False)
    name = string(nullable=False)
    description = string(nullable=False)
    is_active = boolean(nullable=False)
