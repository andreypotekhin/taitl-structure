"""Implicit-feedback document reranking."""

from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.search import (
    DocumentFeedbackOption,
    DocumentSearchCandidate,
    DocumentSearchResult,
    PopularityFeedback,
    QueryDocumentFeedback,
)
from examples.search.schemas.user import BandFallback
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    coalesce,
    inner_join,
    left_join,
    param_join,
    row_number,
    rows_between,
    select_first_qualified,
    unbounded_following,
    unbounded_preceding,
    union_all,
    where,
    window,
    window_max,
)
from structure.plugin.pyspark.dsl.expressions import literal


class RerankDocuments(Transform):
    """Rerank fused retrieval candidates and return the set of search results."""

    maximum_candidates = 1000
    maximum_results = 100

    candidates = input(DocumentSearchCandidate, streaming=True)
    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)
    global_options = lane(DocumentFeedbackOption)
    fallback_options = lane(DocumentFeedbackOption)
    feedback_options = lane(DocumentFeedbackOption)
    query_feedback = lane(QueryDocumentFeedback)
    popularity_feedback = lane(PopularityFeedback)
    scored_candidates = lane(DocumentSearchCandidate)
    normalized_candidates = lane(DocumentSearchCandidate)
    ranked_results = lane(DocumentSearchResult)
    results = output(DocumentSearchResult)

    @step(input=[candidates, band_fallbacks, policy], output=fallback_options)
    def select_fallback_options(
        self, candidate: DocumentSearchCandidate, fallback: BandFallback, policy: RelevancePolicy
    ) -> DocumentFeedbackOption:
        where(candidate.candidate_rank <= self.maximum_candidates, candidate.user_band_id.is_not_null())
        inner_join(fallback, on=fallback.user_band_id == candidate.user_band_id)
        policy = param_join(policy)
        return DocumentFeedbackOption.project(candidate)(
            feedback_band_id=fallback.user_band_fallback_id,
            fallback_ordinal=fallback.ordinal,
            minimum_band_impressions=policy.minimum_band_impressions,
        )

    @step(input=[candidates, policy], output=global_options)
    def select_global_options(
        self, candidate: DocumentSearchCandidate, policy: RelevancePolicy
    ) -> DocumentFeedbackOption:
        where(candidate.candidate_rank <= self.maximum_candidates, candidate.user_band_id.is_null())
        policy = param_join(policy)
        return DocumentFeedbackOption.project(candidate)(
            feedback_band_id=literal(None),
            fallback_ordinal=0,
            minimum_band_impressions=policy.minimum_band_impressions,
        )

    @step(input=[fallback_options, global_options], output=feedback_options)
    def merge_feedback_options(
        self, fallback_option: DocumentFeedbackOption, global_option: DocumentFeedbackOption
    ) -> DocumentFeedbackOption:
        merged = union_all(global_option)
        return DocumentFeedbackOption.project(merged)

    @step(input=[feedback_options, query_document_signals], output=query_feedback)
    def select_query_feedback(
        self, option: DocumentFeedbackOption, signal: QueryDocumentSignals
    ) -> QueryDocumentFeedback:
        left_join(
            signal,
            on=(signal.query == option.query)
            & (signal.document_id == option.document_id)
            & signal.band_id.null_safe_eq(option.feedback_band_id),
        )
        selected = select_first_qualified(
            option.search_query_id,
            option.experiment_id,
            option.user_band_id,
            option.candidate_rank,
            option.document_id,
            where=option.feedback_band_id.is_null() | (signal.impression_count >= option.minimum_band_impressions),
            order_by=option.fallback_ordinal.asc(),
            missing="allow",
        )
        return QueryDocumentFeedback.project(selected)(
            query_feedback=signal.normalized_score,
        )

    @step(input=[feedback_options, document_popularity], output=popularity_feedback)
    def select_popularity_feedback(
        self, option: DocumentFeedbackOption, signal: DocumentPopularity
    ) -> PopularityFeedback:
        left_join(
            signal,
            on=(signal.document_id == option.document_id) & signal.band_id.null_safe_eq(option.feedback_band_id),
        )
        selected = select_first_qualified(
            option.search_query_id,
            option.experiment_id,
            option.user_band_id,
            option.candidate_rank,
            option.document_id,
            where=option.feedback_band_id.is_null() | (signal.impression_count >= option.minimum_band_impressions),
            order_by=option.fallback_ordinal.asc(),
            missing="allow",
        )
        return PopularityFeedback.project(selected)(
            popularity_feedback=signal.normalized_score,
        )

    @step(
        input=[candidates, query_feedback, popularity_feedback, policy],
        output=scored_candidates,
    )
    def score_candidates(
        self,
        candidate: DocumentSearchCandidate,
        query: QueryDocumentFeedback,
        popularity: PopularityFeedback,
        policy: RelevancePolicy,
    ) -> DocumentSearchCandidate:
        where(candidate.candidate_rank <= self.maximum_candidates)
        left_join(
            query,
            on=(query.search_query_id == candidate.search_query_id)
            & query.experiment_id.null_safe_eq(candidate.experiment_id)
            & query.user_band_id.null_safe_eq(candidate.user_band_id)
            & (query.candidate_rank == candidate.candidate_rank)
            & (query.document_id == candidate.document_id),
        )
        left_join(
            popularity,
            on=(popularity.search_query_id == candidate.search_query_id)
            & popularity.experiment_id.null_safe_eq(candidate.experiment_id)
            & popularity.user_band_id.null_safe_eq(candidate.user_band_id)
            & (popularity.candidate_rank == candidate.candidate_rank)
            & (popularity.document_id == candidate.document_id),
        )
        policy = param_join(policy)
        return DocumentSearchCandidate.project(candidate)(
            score_feedback=0.8 * coalesce(query.query_feedback, 0.0)
            + 0.2 * coalesce(popularity.popularity_feedback, 0.0),
            score_rank=0.0,
            score_weight=policy.score_weight,
            feedback_weight=policy.feedback_weight,
        )

    @step(input=scored_candidates, output=normalized_candidates)
    def normalize_score(self, candidate: DocumentSearchCandidate) -> DocumentSearchCandidate:
        """Normalize retrieval scores inside one query, band, and experiment ranking."""

        maximum = window_max(
            candidate.retrieval_score,
            over=window(
                partition_by=(candidate.search_query_id, candidate.user_band_id, candidate.experiment_id),
                order_by=candidate.document_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        return DocumentSearchCandidate.project(candidate)(
            score_rank=candidate.score_weight * candidate.score / maximum
            + candidate.feedback_weight * candidate.score_feedback,
        )

    @step(input=normalized_candidates, output=ranked_results)
    def rank_results(self, candidate: DocumentSearchCandidate) -> DocumentSearchResult:
        """Publish deterministic ranks for one query, band, and experiment."""

        return DocumentSearchResult.project(candidate)(
            rank=row_number(
                partition_by=(candidate.search_query_id, candidate.user_band_id, candidate.experiment_id),
                order_by=(candidate.score_rank.desc_nulls_last(), candidate.document_id.asc_nulls_first()),
            ),
        )

    @step(input=ranked_results, output=results)
    def select_results(self, result: DocumentSearchResult) -> DocumentSearchResult:
        """Return only the final page-sized result set after reranking."""

        where(result.rank <= self.maximum_results)
        return DocumentSearchResult.project(result)
