"""BM25-first document candidate retrieval."""

from examples.search.adoption import SEARCH_STREAMING_CONTRACTS_ENABLED
from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.search import (
    DocumentScore,
    DocumentSearchCandidate,
    DocumentSearchTarget,
    ScorePolicy,
    SearchQuery,
)
from examples.search.schemas.text import Document
from examples.search.schemas.user import BandMembership
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    coalesce,
    cross_join,
    datediff,
    drop_duplicates,
    event_time_between,
    inner_join,
    left_join,
    lower,
    regexp_replace,
    row_number,
    trim,
    union_all,
    watermark,
    where,
)
from structure.plugin.pyspark.dsl.expressions import literal


class RetrieveDocuments(Transform):
    """Select lexical candidates for a given query."""

    maximum_candidates = 1000

    queries = input(SearchQuery, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    documents = input(Document)
    document_scores = input(DocumentScore)
    streamed_documents = input(Document, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    streamed_document_scores = input(DocumentScore, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    online_streamed_document_scores = input(DocumentScore, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    online_document_scores = input(DocumentScore, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    requests = input(SearchRequest, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    band_memberships = input(BandMembership)
    score_policy = input(ScorePolicy)
    prefilter_targets = input(DocumentSearchTarget, streaming=SEARCH_STREAMING_CONTRACTS_ENABLED)
    stored_scores = lane(DocumentScore)
    streamed_scores = lane(DocumentScore)
    stored_candidates = lane(DocumentSearchCandidate)
    streamed_candidates = lane(DocumentSearchCandidate)
    candidates = output(DocumentSearchCandidate)

    @step(input=[document_scores, online_document_scores, requests, score_policy], output=stored_scores)
    def merge_stored_scores(
        self,
        stored: DocumentScore,
        online: DocumentScore,
        request: SearchRequest,
        policy: ScorePolicy,
    ) -> DocumentScore:
        candidate: DocumentScore = union_all(online)
        inner_join(request, on=request.query_id == candidate.query_id)
        cross_join(policy, allow_cartesian=True)
        age = datediff(request.requested_at, candidate.scored_at)
        where(
            (candidate.scored_at <= request.requested_at)
            & (candidate.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
            & candidate.experiment_id.null_safe_eq(request.experiment_id)
        )
        drop_duplicates(candidate.query_id, candidate.document_id, candidate.experiment_id)
        return DocumentScore.project(candidate)

    @step(
        input=[streamed_document_scores, online_streamed_document_scores, requests, score_policy],
        output=streamed_scores,
    )
    def merge_streamed_scores(
        self,
        streamed: DocumentScore,
        online: DocumentScore,
        request: SearchRequest,
        policy: ScorePolicy,
    ) -> DocumentScore:
        candidate: DocumentScore = union_all(online)
        inner_join(request, on=request.query_id == candidate.query_id)
        cross_join(policy, allow_cartesian=True)
        age = datediff(request.requested_at, candidate.scored_at)
        where(
            (candidate.scored_at <= request.requested_at)
            & (candidate.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
            & candidate.experiment_id.null_safe_eq(request.experiment_id)
        )
        drop_duplicates(candidate.query_id, candidate.document_id, candidate.experiment_id)
        return DocumentScore.project(candidate)

    @step(
        input=[documents, stored_scores, queries, requests, band_memberships, prefilter_targets],
        output=stored_candidates,
    )
    def select_stored_candidates(
        self,
        document: Document,
        score: DocumentScore,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
        target: DocumentSearchTarget,
    ) -> DocumentSearchCandidate:
        watermark(query.requested_at, delay="10 minutes")
        watermark(request.requested_at, delay="10 minutes")
        return self._candidate(document, score, query, request, band, target)

    @step(
        input=[streamed_documents, streamed_scores, queries, requests, band_memberships, prefilter_targets],
        output=streamed_candidates,
    )
    def select_streamed_candidates(
        self,
        document: Document,
        score: DocumentScore,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
        target: DocumentSearchTarget,
    ) -> DocumentSearchCandidate:
        watermark(query.requested_at, delay="10 minutes")
        watermark(request.requested_at, delay="10 minutes")
        return self._candidate(document, score, query, request, band, target)

    @step(input=[stored_candidates, streamed_candidates], output=candidates)
    def rank_candidates(
        self, stored: DocumentSearchCandidate, streamed: DocumentSearchCandidate
    ) -> DocumentSearchCandidate:
        candidate: DocumentSearchCandidate = union_all(streamed)
        return DocumentSearchCandidate.project(candidate)(
            candidate_rank=row_number(
                partition_by=(candidate.search_query_id, candidate.user_band_id, candidate.experiment_id),
                order_by=(candidate.score.desc_nulls_last(), candidate.document_id.asc_nulls_first()),
            )
        )

    def _candidate(
        self,
        document: Document,
        score: DocumentScore,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
        target: DocumentSearchTarget,
    ) -> DocumentSearchCandidate:
        inner_join(target, on=(target.query_id == query.id) & (target.document_id == document.id))
        inner_join(on=document.id == score.document_id)
        inner_join(on=query.id == score.query_id)
        inner_join(on=request.query_id == query.id)
        left_join(on=band.user_id == request.user_id)
        where(
            score.score.is_not_null()
            & score.experiment_id.null_safe_eq(request.experiment_id)
            & (query.requested_at == request.requested_at)
            & event_time_between(query.requested_at, request.requested_at, upper="0 seconds")
        )
        return DocumentSearchCandidate.project(score, document)(
            search_query_id=query.id,
            user_band_id=coalesce(band.user_band_id, literal(None)),
            band_id=band.band_id,
            query=lower(regexp_replace(trim(query.content), pattern=r"\s+", replacement=" ")),
            candidate_rank=0,
            document_id=document.id,
            score_feedback=0.0,
            score_rank=0.0,
            score_weight=0.0,
            feedback_weight=0.0,
        )
