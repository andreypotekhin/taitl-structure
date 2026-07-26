"""Experiment metadata for Search score variants."""

from structure import Schema
from structure.plugin.pyspark import *


class Experiment(Schema):
    """One named caller-managed Search experiment."""

    experiment_id = string(nullable=False)
    name = string(nullable=False)
    description = string(nullable=False)
    is_active = boolean(nullable=False)
