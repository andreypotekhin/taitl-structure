"""Impression and click event contracts with daily aggregate facts."""

from structure import Schema
from structure.plugin.pyspark import *


class SearchRequest(Schema):
    """One user search attempt, including attempts with no displayed results."""

    id = string(nullable=False)
    requested_at = timestamp(nullable=False)
    query = string(nullable=False)
    ranking_version = string(nullable=False)


class Impression(Schema):
    """One document displayed for a search request."""

    id = string(nullable=False)
    search_request_id = string(nullable=False)
    shown_at = timestamp(nullable=False)
    query = string(nullable=False)
    document_id = string(nullable=False)
    position = long(nullable=False)
    examination_propensity = double(nullable=False)


class Click(Schema):
    id = string(nullable=False)
    occurred_at = timestamp(nullable=False)
    impression_id = string(nullable=False)
    dwell_seconds = double(nullable=False)


class DailyImpressions(Schema):
    window = struct(TimeWindow, nullable=False)
    query = string(nullable=False)
    document_id = string(nullable=False)
    position = long(nullable=False)
    examination_propensity = double(nullable=False)
    impression_count = long(nullable=False)


class DailyClicks(Schema):
    window = struct(TimeWindow, nullable=False)
    query = string(nullable=False)
    document_id = string(nullable=False)
    position = long(nullable=False)
    examination_propensity = double(nullable=False)
    click_count = long(nullable=False)
    clicked_impression_count = long(nullable=False)
    dwell_seconds = double(nullable=False)
    dwell_credit = double(nullable=False)
    long_click_count = long(nullable=False)
