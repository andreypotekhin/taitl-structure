"""Offline quality and observed-behavior evaluation contracts."""

from structure import Schema
from structure.plugin.pyspark import *


class EvaluationBatch(Schema):
    """One UTC-aligned daily window selected by the caller."""

    window = struct(TimeWindow, nullable=False)


class DocumentRelevanceJudgment(Schema):
    """One caller-supplied four-grade document relevance judgment."""

    search_query_id = string(nullable=False)
    document_id = string(nullable=False)
    relevance_grade = long(nullable=False)


class DocumentQueryEvaluation(Schema):
    """Offline retrieval metrics for one query in one daily batch."""

    window = struct(TimeWindow, nullable=False)
    search_query_id = string(nullable=False)
    returned_result_count = long(nullable=False)
    judged_result_count = long(nullable=False)
    binary_relevant_judgment_count = long(nullable=False)
    covered_at_5 = boolean(nullable=False)
    covered_at_10 = boolean(nullable=False)
    covered_at_15 = boolean(nullable=False)
    reciprocal_rank_covered = boolean(nullable=False)
    precision_at_5 = double(nullable=True)
    precision_at_10 = double(nullable=True)
    precision_at_15 = double(nullable=True)
    judged_recall_at_5 = double(nullable=True)
    judged_recall_at_10 = double(nullable=True)
    judged_recall_at_15 = double(nullable=True)
    success_at_5 = double(nullable=True)
    success_at_10 = double(nullable=True)
    success_at_15 = double(nullable=True)
    ndcg_at_5 = double(nullable=True)
    ndcg_at_10 = double(nullable=True)
    ndcg_at_15 = double(nullable=True)
    reciprocal_rank = double(nullable=True)


class DocumentEvaluationSummary(Schema):
    """Daily means and coverage for one judged document ranking run."""

    window = struct(TimeWindow, nullable=False)
    query_count = long(nullable=False)
    binary_relevant_query_count = long(nullable=False)
    no_binary_relevant_query_count = long(nullable=False)
    eligible_at_5_count = long(nullable=False)
    eligible_at_10_count = long(nullable=False)
    eligible_at_15_count = long(nullable=False)
    reciprocal_rank_eligible_count = long(nullable=False)
    precision_at_5 = double(nullable=True)
    precision_at_10 = double(nullable=True)
    precision_at_15 = double(nullable=True)
    judged_recall_at_5 = double(nullable=True)
    judged_recall_at_10 = double(nullable=True)
    judged_recall_at_15 = double(nullable=True)
    success_at_5 = double(nullable=True)
    success_at_10 = double(nullable=True)
    success_at_15 = double(nullable=True)
    ndcg_at_5 = double(nullable=True)
    ndcg_at_10 = double(nullable=True)
    ndcg_at_15 = double(nullable=True)
    mean_reciprocal_rank = double(nullable=True)


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


class EvaluationQuery(Schema):
    window = struct(TimeWindow, nullable=False)
    search_query_id = string(nullable=False)


class EvaluationResult(EvaluationQuery):
    document_id = string(nullable=True)
    rank = long(nullable=True)
    relevance_grade = long(nullable=True)


class EvaluationJudgment(EvaluationQuery):
    relevance_grade = long(nullable=False)
    ideal_rank = long(nullable=False)


class EvaluationJudgmentTotals(EvaluationQuery):
    binary_relevant_judgment_count = long(nullable=False)


class EvaluationIdealDcg(EvaluationQuery):
    ideal_dcg_at_5 = double(nullable=False)
    ideal_dcg_at_10 = double(nullable=False)
    ideal_dcg_at_15 = double(nullable=False)


class EvaluationResultTotals(EvaluationQuery):
    returned_result_count = long(nullable=False)
    judged_result_count = long(nullable=False)
    unjudged_result_count = long(nullable=False)
    unjudged_at_5 = long(nullable=False)
    unjudged_at_10 = long(nullable=False)
    unjudged_at_15 = long(nullable=False)
    relevant_at_5 = long(nullable=False)
    relevant_at_10 = long(nullable=False)
    relevant_at_15 = long(nullable=False)
    first_relevant_rank = long(nullable=True)
    dcg_at_5 = double(nullable=True)
    dcg_at_10 = double(nullable=True)
    dcg_at_15 = double(nullable=True)
