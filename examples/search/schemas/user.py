"""Caller-owned user profiles, bands, and derived user-band contexts."""

from structure import Schema
from structure.plugin.pyspark import *


class User(Schema):
    """Caller-owned user profile."""

    id = string(nullable=False)
    age = long(nullable=True)
    gender = string(nullable=True)
    locale = string(nullable=True)
    country = string(nullable=True)
    geo_tag = string(nullable=True)
    device_type = string(nullable=True)
    time_zone = string(nullable=True)


class Band(Schema):
    """A small caller-owned demographic band catalog; it is not event data."""

    id = string(nullable=False)
    name = string(nullable=True)
    priority = long(nullable=False)
    parent_band_id = string(nullable=True)
    age_start = long(nullable=True)
    age_end = long(nullable=True)
    genders = array(string(), contains_null=False, nullable=False)
    locales = array(string(), contains_null=False, nullable=False)
    countries = array(string(), contains_null=False, nullable=False)
    geo_tags = array(string(), contains_null=False, nullable=False)
    device_types = array(string(), contains_null=False, nullable=False)
    time_zones = array(string(), contains_null=False, nullable=False)


class BandMembership(Schema):
    """A user's direct or inherited caller band and its singleton user band."""

    user_id = string(nullable=False)
    band_id = string(nullable=True)
    user_band_id = string(nullable=False)


class UserBand(Schema):
    """One reusable resolved user-band context."""

    user_band_id = string(nullable=False)
    band_ids = array(string(), contains_null=False, nullable=False)


class UserBandMembership(Schema):
    """A user and the reusable context resolved from their most-specific bands."""

    user_id = string(nullable=False)
    user_band_id = string(nullable=True)


class BandFallback(Schema):
    """An ordered fallback chain between reusable user bands.

    ``user_band_id + ordinal`` resolves to ``user_band_fallback_id``. Both IDs
    refer to rows in :class:`UserBand`; a null fallback ID denotes global.
    """

    user_band_id = string(nullable=False)
    ordinal = long(nullable=False)
    user_band_fallback_id = string(nullable=True)
