"""Implicit-feedback document reranking."""

from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.search import DocumentSearchCandidate, DocumentSearchResult
from examples.search.transforms.searching.search_docs.RetrieveDocuments import RetrieveDocuments
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    coalesce,
    cross_join,
    left_join,
    row_number,
    rows_between,
    unbounded_following,
    unbounded_preceding,
    where,
    window,
    window_max,
)


class RerankDocuments(Transform):
    """Enrich lexical candidates with feedback and determine final rank."""

    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    policy = input(RelevancePolicy)
    scored_candidates = lane(DocumentSearchCandidate)
    normalized_candidates = lane(DocumentSearchCandidate)
    results = output(DocumentSearchResult)

    @step(
        input=[lane(RetrieveDocuments.candidates), query_document_signals, document_popularity, policy],
        output=scored_candidates,
    )
    def score_candidates(
        self,
        candidate: DocumentSearchCandidate,
        query_signal: QueryDocumentSignals,
        popularity: DocumentPopularity,
        policy: RelevancePolicy,
    ) -> DocumentSearchCandidate:
        """Enrich one global or reusable band-context candidate with feedback."""

        where(candidate.candidate_rank <= RetrieveDocuments.maximum_candidates)
        query_signal = left_join(
            query_signal,
            on=(query_signal.query == candidate.query)
            & (query_signal.document_id == candidate.document_id)
            & query_signal.band_id.null_safe_eq(candidate.band_id),
        )
        left_join(
            on=(popularity.document_id == candidate.document_id)
            & popularity.band_id.null_safe_eq(candidate.band_id)
        )
        policy = cross_join(policy, allow_cartesian=True)
        feedback = 0.8 * coalesce(query_signal.normalized_score, 0.0) + 0.2 * coalesce(popularity.normalized_score, 0.0)
        return DocumentSearchCandidate.project(candidate)(
            score_feedback=feedback,
            score_rank=0.0,
            score_weight=policy.score_weight,
            feedback_weight=policy.feedback_weight,
        )

    @step(input=scored_candidates, output=normalized_candidates)
    def normalize_score(self, candidate: DocumentSearchCandidate) -> DocumentSearchCandidate:
        """Normalize lexical scores inside one query, context, and experiment ranking."""

        maximum = window_max(
            candidate.score,
            over=window(
                partition_by=(candidate.search_query_id, candidate.band_id, candidate.experiment_id),
                order_by=candidate.document_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        return DocumentSearchCandidate.project(candidate)(
            score_rank=candidate.score_weight * candidate.score / maximum
            + candidate.feedback_weight * candidate.score_feedback,
        )

    @step(input=normalized_candidates, output=results)
    def rank_results(self, candidate: DocumentSearchCandidate) -> DocumentSearchResult:
        """Publish deterministic ranks for one query, reusable context, and experiment."""

        return DocumentSearchResult.base(candidate)(
            rank=row_number(
                partition_by=(candidate.search_query_id, candidate.band_id, candidate.experiment_id),
                order_by=(candidate.score_rank.desc_nulls_last(), candidate.document_id.asc_nulls_first()),
            ),
        )
