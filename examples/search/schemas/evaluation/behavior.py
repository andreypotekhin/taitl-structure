"""Observed document-search behavior contracts."""

from structure import Schema
from structure.plugin.pyspark import *


class DocumentSearchRequestBehavior(Schema):
    """Observed behavior for one served document result list."""

    window = struct(TimeWindow, nullable=False)
    search_request_id = string(nullable=False)
    ranking_version = string(nullable=False)
    query = string(nullable=False)
    result_count = long(nullable=False)
    clicked_result_count = long(nullable=False)
    long_clicked_result_count = long(nullable=False)
    has_click = boolean(nullable=True)
    has_long_click = boolean(nullable=True)
    first_click_rank = long(nullable=True)
    first_long_click_rank = long(nullable=True)
    reciprocal_first_long_click_rank = double(nullable=False)


class DailyDocumentSearchBehavior(Schema):
    """Observed daily behavior summary for one ranking version."""

    window = struct(TimeWindow, nullable=False)
    ranking_version = string(nullable=False)
    request_count = long(nullable=False)
    zero_result_request_count = long(nullable=False)
    clicked_request_count = long(nullable=False)
    long_clicked_request_count = long(nullable=False)
    no_click_request_count = long(nullable=False)
    no_long_click_request_count = long(nullable=False)
    raw_click_count = long(nullable=False)
    raw_long_click_count = long(nullable=False)
    mean_first_click_rank = double(nullable=True)
    mean_first_long_click_rank = double(nullable=True)
    mean_reciprocal_first_long_click_rank = double(nullable=False)
    ips_long_click_rate = double(nullable=True)
    ips_dwell_credit_per_impression = double(nullable=True)


class BehaviorRequest(Schema):
    window = struct(TimeWindow, nullable=False)
    search_request_id = string(nullable=False)
    ranking_version = string(nullable=False)
    query = string(nullable=False)


class BehaviorImpression(BehaviorRequest):
    impression_id = string(nullable=False)
    shown_at = timestamp(nullable=False)
    document_id = string(nullable=False)
    position = long(nullable=False)
    examination_propensity = double(nullable=False)
    click_count = long(nullable=False)
    long_click_count = long(nullable=False)
    dwell_credit = double(nullable=False)


class BehaviorExposure(Schema):
    window = struct(TimeWindow, nullable=False)
    ranking_version = string(nullable=False)
    ips_impression_weight = double(nullable=False)
    ips_long_click_weight = double(nullable=False)
    ips_dwell_credit = double(nullable=False)


class BehaviorRequestMetrics(DocumentSearchRequestBehavior):
    raw_click_count = long(nullable=False)
    raw_long_click_count = long(nullable=False)


class BehaviorRequestTotals(BehaviorRequestMetrics):
    pass


class BehaviorDailyCounts(DailyDocumentSearchBehavior):
    pass
