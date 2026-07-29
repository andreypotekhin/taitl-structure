"""Impression and click event contracts with daily aggregate facts."""

from structure import Schema
from structure.plugin.pyspark import *


class SearchRequest(Schema):
    """One user search attempt, including attempts with no displayed results."""

    id = string(nullable=False)
    query_id = string(nullable=False)
    query = string(nullable=False)
    user_id = string(nullable=True)
    experiment_id = string(nullable=True)
    ranking_version = string(nullable=False)
    requested_at = timestamp(nullable=False)


class Impression(Schema):
    """One document displayed for a search request."""

    id = string(nullable=False)
    search_request_id = string(nullable=False)
    query = string(nullable=False)
    document_id = string(nullable=False)
    position = long(nullable=False)
    examination_propensity = double(nullable=False)
    user_id = string(nullable=True)
    shown_at = timestamp(nullable=False)


class Click(Schema):
    """One caller-recorded action against an impression."""

    id = string(nullable=False)
    impression_id = string(nullable=False)
    user_id = string(nullable=True)
    dwell_seconds = double(nullable=False)
    occurred_at = timestamp(nullable=False)


class DailyImpressions(Schema):
    window = struct(TimeWindow, nullable=False)
    query = string(nullable=False)
    document_id = string(nullable=False)
    position = long(nullable=False)
    examination_propensity = double(nullable=False)
    user_id = string(nullable=True)
    band_id = string(nullable=True)
    impression_count = long(nullable=False)


class DailyClicks(Schema):
    window = struct(TimeWindow, nullable=False)
    query = string(nullable=False)
    document_id = string(nullable=False)
    position = long(nullable=False)
    examination_propensity = double(nullable=False)
    user_id = string(nullable=True)
    band_id = string(nullable=True)
    click_count = long(nullable=False)
    clicked_impression_count = long(nullable=False)
    dwell_seconds = double(nullable=False)
    dwell_credit = double(nullable=False)
    long_click_count = long(nullable=False)
