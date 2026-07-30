"""Experiment searching001: favor query-document feedback more strongly during reranking."""

from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.search import DocumentSearchCandidate, PopularityFeedback, QueryDocumentFeedback
from examples.search.transforms.searching.search_docs.rerank import RerankDocuments
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments
from structure import stage, step
from structure.plugin.pyspark import coalesce, cross_join, left_join, where


class Searching001AdjustRerankDocuments(RerankDocuments):
    """Favor query-specific feedback over global popularity."""

    @step(input=[RerankDocuments.overlapped_candidates, RerankDocuments.query_feedback, RerankDocuments.popularity_feedback, RerankDocuments.policy], output=RerankDocuments.scored_candidates)
    def score_candidates(
        self,
        candidate: DocumentSearchCandidate,
        query: QueryDocumentFeedback,
        popularity: PopularityFeedback,
        policy: RelevancePolicy,
    ) -> DocumentSearchCandidate:
        where(candidate.candidate_rank <= 100)
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
        policy = cross_join(policy, allow_cartesian=True)
        return DocumentSearchCandidate.project(candidate)(
            experiment_id="Searching001AdjustRerankSearchDocuments",
            score_feedback=0.9 * coalesce(query.query_feedback, 0.0)
            + 0.1 * coalesce(popularity.popularity_feedback, 0.0),
            score_rank=0.0,
            score_weight=policy.score_weight,
            feedback_weight=policy.feedback_weight,
        )


class Searching001AdjustRerankSearchDocuments(SearchDocuments):
    """Run document search with the searching001 reranking stage."""

    reranked = stage(
        Searching001AdjustRerankDocuments(
            overlapped_candidates=SearchDocuments.overlapped.overlapped_candidates,
            query_document_signals=SearchDocuments.query_document_signals,
            document_popularity=SearchDocuments.document_popularity,
            band_fallbacks=SearchDocuments.band_fallbacks,
            policy=SearchDocuments.policy,
        )
    )
