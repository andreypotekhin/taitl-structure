"""Persistable Search ranking-model contract."""

from structure import Schema
from structure.plugin.pyspark import double, integer, map, string


class RankingArtifact(Schema):
    """One manually promoted, standardized linear ranking artifact."""

    model_id = string(nullable=False)
    ranker_id = string(nullable=False)
    artifact_version = integer(nullable=False)
    feature_contract_version = string(nullable=False)
    intercept = double(nullable=False)
    means = map(string(), double(), value_contains_null=False, nullable=False)
    scales = map(string(), double(), value_contains_null=False, nullable=False)
    weights = map(string(), double(), value_contains_null=False, nullable=False)
    snapshot_id = string(nullable=False)
    split_seed = string(nullable=False)
    metadata = map(string(), string(), value_contains_null=False, nullable=False)
