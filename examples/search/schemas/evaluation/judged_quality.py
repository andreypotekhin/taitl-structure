"""Judged document-ranking quality contracts."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from structure import Schema
from structure.plugin.pyspark import *


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
