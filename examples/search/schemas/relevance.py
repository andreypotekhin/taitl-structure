"""Batch relevance policy and scoring contracts."""

from structure import Schema
from structure.plugin.pyspark import *


class RelevancePolicy(Schema):
    half_life_days = double(nullable=False)
    score_weight = double(nullable=False)
    feedback_weight = double(nullable=False)
    dwell_feedback_weight = double(nullable=False)
    ctr_feedback_weight = double(nullable=False)
    minimum_ctr_impressions = long(nullable=False)
    minimum_band_impressions = long(nullable=False)
    evaluated_at = timestamp(nullable=False)


class QueryDocumentSignals(Schema):
    """Query/document feedback signal."""

    query = string(nullable=False)
    document_id = string(nullable=False)
    band_id = string(nullable=True)
    impression_count = long(nullable=False)
    click_count = long(nullable=False)
    clicked_impression_count = long(nullable=False)
    dwell_seconds = double(nullable=False)
    long_click_count = long(nullable=False)
    click_through_rate = double(nullable=False)
    ips_clicks = double(nullable=False)
    ips_dwell_credit = double(nullable=False)
    ips_click_through_rate = double(nullable=False)
    ips_impression_weight = double(nullable=False)
    ips_clicked_impression_weight = double(nullable=False)
    normalized_dwell_score = double(nullable=False)
    normalized_ctr_score = double(nullable=False)
    normalized_score = double(nullable=False)


class DocumentPopularity(Schema):
    """One document feedback signal."""

    document_id = string(nullable=False)
    band_id = string(nullable=True)
    impression_count = long(nullable=False)
    click_count = long(nullable=False)
    clicked_impression_count = long(nullable=False)
    dwell_seconds = double(nullable=False)
    long_click_count = long(nullable=False)
    click_through_rate = double(nullable=False)
    ips_clicks = double(nullable=False)
    ips_dwell_credit = double(nullable=False)
    ips_click_through_rate = double(nullable=False)
    ips_impression_weight = double(nullable=False)
    ips_clicked_impression_weight = double(nullable=False)
    normalized_dwell_score = double(nullable=False)
    normalized_ctr_score = double(nullable=False)
    normalized_score = double(nullable=False)
