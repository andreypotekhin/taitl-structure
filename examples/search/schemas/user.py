"""Caller-owned user profiles, cohorts, and derived relevance bands."""

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


class Cohort(Schema):
    """A small caller-owned cohort configuration catalog; it is not event data."""

    id = string(nullable=False)
    name = string(nullable=True)
    priority = long(nullable=False)
    parent_cohort_id = string(nullable=True)
    age_start = long(nullable=True)
    age_end = long(nullable=True)
    genders = array(string(), contains_null=False, nullable=False)
    locales = array(string(), contains_null=False, nullable=False)
    countries = array(string(), contains_null=False, nullable=False)
    geo_tags = array(string(), contains_null=False, nullable=False)
    device_types = array(string(), contains_null=False, nullable=False)
    time_zones = array(string(), contains_null=False, nullable=False)


class CohortMembership(Schema):
    """Most-specific cohort matched by user profile."""

    user_id = string(nullable=False)
    cohort_id = string(nullable=False)
    parent_cohort_id = string(nullable=True)
    priority = long(nullable=False)


class CohortLineage(Schema):
    """A matched leaf cohort and one cohort it satisfies, including itself."""

    cohort_id = string(nullable=False)
    ancestor_cohort_id = string(nullable=False)


class Band(Schema):
    """Set of most-specific matched cohorts."""

    band_id = string(nullable=False)
    cohort_ids = array(string(), contains_null=False, nullable=False)


class UserBand(Schema):
    """One user's resolved band; null denotes global."""

    user_id = string(nullable=False)
    band_id = string(nullable=True)


class BandFallback(Schema):
    """Fallback candidate for a band."""

    band_id = string(nullable=False)
    fallback_band_id = string(nullable=True)
    ordinal = long(nullable=False)
