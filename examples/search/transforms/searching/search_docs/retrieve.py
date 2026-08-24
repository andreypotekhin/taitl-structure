"""Materialize lexical and vector document candidate lanes."""

from examples.search.schemas.clicks import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.schemas.text import *
from examples.search.schemas.user import *
from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl.expressions import *


class RetrieveDocuments(Transform):
    """Materialize lexical and vector candidates."""

    queries = input(SearchQuery, streaming=True)
    documents = input(Document)
    streamed_documents = input(Document, streaming=True)
    online_document_scores = input(DocumentScore, streaming=True)
    requests = input(SearchRequest, streaming=True)
    band_memberships = input(BandMembership)
    score_policy = input(ScorePolicy)
    document_vector_scores = input(DocumentVectorScore)
    vector_policy = input(VectorIndexPolicy)
    prefilter_targets = input(DocumentSearchTarget, streaming=True)
    stored_candidates = lane(DocumentSearchCandidate)
    streamed_candidates = lane(DocumentSearchCandidate)
    vector_candidates = output(DocumentSearchCandidate)
    candidates = output(DocumentSearchCandidate)

    @step(
        input=[documents, online_document_scores, queries, requests, band_memberships, prefilter_targets],
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
        input=[streamed_documents, online_document_scores, queries, requests, band_memberships, prefilter_targets],
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
    def merge_candidates(
        self, stored: DocumentSearchCandidate, streamed: DocumentSearchCandidate
    ) -> DocumentSearchCandidate:
        candidate: DocumentSearchCandidate = union_all(streamed)
        return DocumentSearchCandidate.project(candidate)(
            retrieval_score=candidate.score,
        )

    @step(
        input=[document_vector_scores, documents, queries, requests, band_memberships, vector_policy],
        output=vector_candidates,
    )
    def select_vector_candidates(
        self,
        vector: DocumentVectorScore,
        document: Document,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
        policy: VectorIndexPolicy,
    ) -> DocumentSearchCandidate:
        """Join vector scores to documents."""

        param_join(policy)
        inner_join(document, on=document.id == vector.document_id)
        inner_join(query, on=query.id == vector.query_id)
        inner_join(request, on=request.query_id == query.id)
        left_join(on=band.user_id == request.user_id)
        where(
            (vector.model_id == policy.model_id)
            & (vector.dimension == policy.dimension)
            & (vector.content_revision == policy.content_revision)
            & (vector.experiment_id == policy.experiment_id)
            & vector.experiment_id.null_safe_eq(request.experiment_id)
            & (query.requested_at == request.requested_at)
            & event_time_between(query.requested_at, request.requested_at, upper="0 seconds")
        )
        return DocumentSearchCandidate.project(document)(
            search_query_id=query.id,
            experiment_id=vector.experiment_id,
            user_band_id=coalesce(band.user_band_id, literal(None)),
            band_id=band.band_id,
            query=lower(regexp_replace(trim(query.content), pattern=r"\s+", replacement=" ")),
            candidate_rank=literal(0).cast(types.long()),
            document_id=document.id,
            score=0.0,
            retrieval_score=0.0,
            score_feedback=0.0,
            score_rank=0.0,
            score_weight=0.0,
            feedback_weight=0.0,
            lexical_rank=None,
            vector_rank=None,
            vector_similarity=vector.cosine_similarity,
            rrf_score=0.0,
            rrf_k=literal(0).cast(types.long()),
            vector_backend=vector.vector_backend,
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
        inner_join(on=document.id == score.document_id)
        inner_join(on=query.id == score.query_id)
        inner_join(on=request.query_id == query.id)
        inner_join(target, on=(target.query_id == query.id) & (target.document_id == document.id))
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
            candidate_rank=literal(0).cast(types.long()),
            retrieval_score=score.score,
            document_id=document.id,
            score_feedback=0.0,
            score_rank=0.0,
            score_weight=0.0,
            feedback_weight=0.0,
            lexical_rank=None,
            vector_rank=None,
            vector_similarity=None,
            rrf_score=0.0,
            rrf_k=literal(0).cast(types.long()),
            vector_backend=None,
        )
