"""Judged offline document-ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import (
    DocumentEvaluationSummary,
    DocumentQueryEvaluation,
    DocumentRelevanceJudgment,
    EvaluationIdealDcg,
    EvaluationJudgment,
    EvaluationJudgmentTotals,
    EvaluationQuery,
    EvaluationResult,
    EvaluationResultTotals,
)
from examples.search.schemas.search import DocumentSearchResult, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    avg,
    coalesce,
    count,
    cross_join,
    group_by,
    inner_join,
    left_join,
    log,
    min,
    pow,
    row_number,
    sum,
    when,
    where,
)


class EvaluateDocumentRankingQuality(Transform):
    """Evaluate document ranking run against caller-supplied judgments."""

    batch = input(EvaluationBatch)
    queries = input(SearchQuery)
    results = input(DocumentSearchResult)
    judgments = input(DocumentRelevanceJudgment)
    evaluated_queries = lane(EvaluationQuery)
    evaluated_results = lane(EvaluationResult)
    ranked_judgments = lane(EvaluationJudgment)
    judgment_totals = lane(EvaluationJudgmentTotals)
    ideal_dcgs = lane(EvaluationIdealDcg)
    result_totals = lane(EvaluationResultTotals)
    metrics = lane(DocumentQueryEvaluation)
    query_evaluations = output(DocumentQueryEvaluation)
    summary = output(DocumentEvaluationSummary)

    @step(input=[queries, batch], output=evaluated_queries)
    def select_queries(self, query: SearchQuery, batch: EvaluationBatch) -> EvaluationQuery:
        batch = cross_join(batch, allow_cartesian=True)
        return EvaluationQuery(
            window=batch.window,
            params=None,
            experiment_id="",
            band_id=None,
            search_query_id=query.id,
        )

    @step(input=[evaluated_queries, results, judgments], output=evaluated_results)
    def select_results(
        self,
        query: EvaluationQuery,
        result: DocumentSearchResult,
        judgment: DocumentRelevanceJudgment,
    ) -> EvaluationResult:
        left_join(on=result.search_query_id == query.search_query_id)
        where((result.experiment_id == "") & result.band_id.null_safe_eq(query.band_id))
        left_join(
            on=(judgment.search_query_id == query.search_query_id) & (judgment.document_id == result.document_id),
        )
        return EvaluationResult.project(query, result, judgment)(
            experiment_id=query.experiment_id,
            band_id=query.band_id,
            search_query_id=query.search_query_id,
            document_id=result.document_id,
        )

    @step(input=[evaluated_queries, judgments], output=ranked_judgments)
    def rank_judgments(self, query: EvaluationQuery, judgment: DocumentRelevanceJudgment) -> EvaluationJudgment:
        inner_join(on=judgment.search_query_id == query.search_query_id)
        return EvaluationJudgment.project(query, judgment)(
            search_query_id=query.search_query_id,
            ideal_rank=row_number(
                partition_by=(query.search_query_id, query.band_id),
                order_by=judgment.relevance_grade,
                descending=True,
            ),
        )

    @step(input=ranked_judgments, output=judgment_totals)
    def count_judgments(self, judgment: EvaluationJudgment) -> EvaluationJudgmentTotals:
        group_by(
            window=judgment.window,
            params=judgment.params,
            experiment_id=judgment.experiment_id,
            band_id=judgment.band_id,
            search_query_id=judgment.search_query_id,
        )
        return EvaluationJudgmentTotals.project(judgment)(
            binary_relevant_judgment_count=sum(when(judgment.relevance_grade >= 2, 1).otherwise(0)),
        )

    @step(input=ranked_judgments, output=ideal_dcgs)
    def calculate_ideal_dcg(self, judgment: EvaluationJudgment) -> EvaluationIdealDcg:
        group_by(
            window=judgment.window,
            params=judgment.params,
            experiment_id=judgment.experiment_id,
            band_id=judgment.band_id,
            search_query_id=judgment.search_query_id,
        )
        gain = (pow(2.0, judgment.relevance_grade) - 1.0) / log(judgment.ideal_rank + 1.0, base=2)
        return EvaluationIdealDcg.project(judgment)(
            ideal_dcg_at_5=sum(when(judgment.ideal_rank <= 5, gain).otherwise(0.0)),
            ideal_dcg_at_10=sum(when(judgment.ideal_rank <= 10, gain).otherwise(0.0)),
            ideal_dcg_at_15=sum(when(judgment.ideal_rank <= 15, gain).otherwise(0.0)),
        )

    @step(input=evaluated_results, output=result_totals)
    def total_results(self, result: EvaluationResult) -> EvaluationResultTotals:
        group_by(
            window=result.window,
            params=result.params,
            experiment_id=result.experiment_id,
            band_id=result.band_id,
            search_query_id=result.search_query_id,
        )
        returned = result.document_id.is_not_null()
        judged = result.relevance_grade.is_not_null()
        relevant = result.relevance_grade >= 2
        gain = (pow(2.0, coalesce(result.relevance_grade, 0)) - 1.0) / log(result.rank + 1.0, base=2)
        return EvaluationResultTotals.project(result)(
            returned_result_count=sum(when(returned, 1).otherwise(0)),
            judged_result_count=sum(when(judged, 1).otherwise(0)),
            unjudged_result_count=sum(when(returned & ~judged, 1).otherwise(0)),
            unjudged_at_5=sum(when((result.rank <= 5) & returned & ~judged, 1).otherwise(0)),
            unjudged_at_10=sum(when((result.rank <= 10) & returned & ~judged, 1).otherwise(0)),
            unjudged_at_15=sum(when((result.rank <= 15) & returned & ~judged, 1).otherwise(0)),
            relevant_at_5=sum(when((result.rank <= 5) & relevant, 1).otherwise(0)),
            relevant_at_10=sum(when((result.rank <= 10) & relevant, 1).otherwise(0)),
            relevant_at_15=sum(when((result.rank <= 15) & relevant, 1).otherwise(0)),
            first_relevant_rank=min(result.rank, where=relevant),
            dcg_at_5=sum(when(result.rank <= 5, gain).otherwise(0.0)),
            dcg_at_10=sum(when(result.rank <= 10, gain).otherwise(0.0)),
            dcg_at_15=sum(when(result.rank <= 15, gain).otherwise(0.0)),
        )

    @step(input=[evaluated_queries, result_totals, judgment_totals, ideal_dcgs], output=metrics)
    def calculate_metrics(
        self,
        query: EvaluationQuery,
        results: EvaluationResultTotals,
        judgments: EvaluationJudgmentTotals,
        ideal: EvaluationIdealDcg,
    ) -> DocumentQueryEvaluation:
        left_join(
            on=(results.search_query_id == query.search_query_id) & (results.experiment_id == query.experiment_id)
        )
        left_join(
            on=(judgments.search_query_id == query.search_query_id) & (judgments.experiment_id == query.experiment_id)
        )
        left_join(on=(ideal.search_query_id == query.search_query_id) & (ideal.experiment_id == query.experiment_id))
        relevant_count = coalesce(judgments.binary_relevant_judgment_count, 0)
        reciprocal_rank_covered = coalesce(results.unjudged_result_count, 0) == 0
        return DocumentQueryEvaluation.project(query)(
            returned_result_count=coalesce(results.returned_result_count, 0),
            judged_result_count=coalesce(results.judged_result_count, 0),
            binary_relevant_judgment_count=relevant_count,
            covered_at_5=coalesce(results.unjudged_at_5, 0) == 0,
            covered_at_10=coalesce(results.unjudged_at_10, 0) == 0,
            covered_at_15=coalesce(results.unjudged_at_15, 0) == 0,
            reciprocal_rank_covered=reciprocal_rank_covered,
            precision_at_5=self._precision(results, relevant_count, 5),
            precision_at_10=self._precision(results, relevant_count, 10),
            precision_at_15=self._precision(results, relevant_count, 15),
            judged_recall_at_5=self._recall(results, relevant_count, 5),
            judged_recall_at_10=self._recall(results, relevant_count, 10),
            judged_recall_at_15=self._recall(results, relevant_count, 15),
            success_at_5=self._success(results, relevant_count, 5),
            success_at_10=self._success(results, relevant_count, 10),
            success_at_15=self._success(results, relevant_count, 15),
            ndcg_at_5=self._ndcg(results, ideal, relevant_count, 5),
            ndcg_at_10=self._ndcg(results, ideal, relevant_count, 10),
            ndcg_at_15=self._ndcg(results, ideal, relevant_count, 15),
            reciprocal_rank=when(
                (relevant_count > 0) & reciprocal_rank_covered,
                when(results.first_relevant_rank.is_not_null(), 1.0 / results.first_relevant_rank).otherwise(0.0),
            ).otherwise(None),
        )

    def _precision(self, results: EvaluationResultTotals, relevant_count, cutoff: int):
        covered = getattr(results, f"unjudged_at_{cutoff}") == 0
        relevant = getattr(results, f"relevant_at_{cutoff}")
        return when((relevant_count > 0) & covered, relevant / float(cutoff)).otherwise(None)

    def _recall(self, results: EvaluationResultTotals, relevant_count, cutoff: int):
        covered = getattr(results, f"unjudged_at_{cutoff}") == 0
        relevant = getattr(results, f"relevant_at_{cutoff}")
        return when((relevant_count > 0) & covered, relevant / relevant_count).otherwise(None)

    def _success(self, results: EvaluationResultTotals, relevant_count, cutoff: int):
        covered = getattr(results, f"unjudged_at_{cutoff}") == 0
        relevant = getattr(results, f"relevant_at_{cutoff}")
        return when((relevant_count > 0) & covered, when(relevant > 0, 1.0).otherwise(0.0)).otherwise(None)

    def _ndcg(self, results: EvaluationResultTotals, ideal: EvaluationIdealDcg, relevant_count, cutoff: int):
        covered = getattr(results, f"unjudged_at_{cutoff}") == 0
        actual = getattr(results, f"dcg_at_{cutoff}")
        maximum = getattr(ideal, f"ideal_dcg_at_{cutoff}")
        return when((relevant_count > 0) & covered & (maximum > 0.0), actual / maximum).otherwise(None)

    @step(input=metrics, output=query_evaluations)
    def publish_metrics(self, metric: DocumentQueryEvaluation) -> DocumentQueryEvaluation:
        return DocumentQueryEvaluation.project(metric)

    @step(input=metrics, output=summary)
    def summarize(self, metric: DocumentQueryEvaluation) -> DocumentEvaluationSummary:
        group_by(
            window=metric.window,
            params=metric.params,
            experiment_id=metric.experiment_id,
        )
        return DocumentEvaluationSummary(
            window=metric.window,
            params=metric.params,
            experiment_id=metric.experiment_id,
            query_count=count(),
            binary_relevant_query_count=sum(when(metric.binary_relevant_judgment_count > 0, 1).otherwise(0)),
            no_binary_relevant_query_count=sum(when(metric.binary_relevant_judgment_count == 0, 1).otherwise(0)),
            eligible_at_5_count=sum(when(metric.precision_at_5.is_not_null(), 1).otherwise(0)),
            eligible_at_10_count=sum(when(metric.precision_at_10.is_not_null(), 1).otherwise(0)),
            eligible_at_15_count=sum(when(metric.precision_at_15.is_not_null(), 1).otherwise(0)),
            reciprocal_rank_eligible_count=sum(when(metric.reciprocal_rank.is_not_null(), 1).otherwise(0)),
            precision_at_5=avg(metric.precision_at_5),
            precision_at_10=avg(metric.precision_at_10),
            precision_at_15=avg(metric.precision_at_15),
            judged_recall_at_5=avg(metric.judged_recall_at_5),
            judged_recall_at_10=avg(metric.judged_recall_at_10),
            judged_recall_at_15=avg(metric.judged_recall_at_15),
            success_at_5=avg(metric.success_at_5),
            success_at_10=avg(metric.success_at_10),
            success_at_15=avg(metric.success_at_15),
            ndcg_at_5=avg(metric.ndcg_at_5),
            ndcg_at_10=avg(metric.ndcg_at_10),
            ndcg_at_15=avg(metric.ndcg_at_15),
            mean_reciprocal_rank=avg(metric.reciprocal_rank),
        )
