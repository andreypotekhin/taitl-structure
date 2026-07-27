"""Intermediate schemas for resolving cohort bands."""

from structure import Schema
from structure.plugin.pyspark import array, long, string


class BandMatch(Schema):
    """Internal user-to-band match before leaf pruning."""

    user_id = string(nullable=False)
    band_id = string(nullable=False)
    priority = long(nullable=False)
    parent_band_id = string(nullable=True)


class BandAncestor(Schema):
    """Internal bounded band lineage row."""

    band_id = string(nullable=False)
    ancestor_band_id = string(nullable=False)
    depth = long(nullable=False)


class UserBandPath(Schema):
    """Internal ordered leaf-band path for one user."""

    user_id = string(nullable=False)
    band_ids = array(string(), contains_null=False, nullable=False)


class SingletonUserBand(Schema):
    """Internal singleton user-band row keyed by its caller band."""

    band_id = string(nullable=False)
    user_band_id = string(nullable=False)
    band_ids = array(string(), contains_null=False, nullable=False)
