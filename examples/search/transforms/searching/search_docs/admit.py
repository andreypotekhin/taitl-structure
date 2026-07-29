"""BM25-first document candidate retrieval."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.search import DocumentScore, DocumentSearchCandidate, SearchQuery
from examples.search.schemas.text import Document
from examples.search.schemas.user import BandMembership
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    coalesce,
    inner_join,
    left_join,
    lower,
    regexp_replace,
    row_number,
    trim,
    union_all,
    where,
)
from structure.plugin.pyspark.dsl.expressions import literal


class RetrieveDocuments(Transform):
    """Select lexical candidates for a given query."""

    maximum_candidates = 1000

    queries = input(SearchQuery)
    documents = input(Document)
    document_scores = input(DocumentScore)
    streamed_documents = input(Document, streaming=True)
    streamed_document_scores = input(DocumentScore, streaming=True)
    requests = input(SearchRequest)
    band_memberships = input(BandMembership)
    stored_candidates = lane(DocumentSearchCandidate)
    streamed_candidates = lane(DocumentSearchCandidate)
    candidates = output(DocumentSearchCandidate)

    @step(input=[documents, document_scores, queries, requests, band_memberships], output=stored_candidates)
    def select_stored_candidates(
        self,
        document: Document,
        score: DocumentScore,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
    ) -> DocumentSearchCandidate:
        return self._candidate(document, score, query, request, band)

    @step(input=[streamed_documents, streamed_document_scores, queries, requests, band_memberships], output=streamed_candidates)
    def select_streamed_candidates(
        self,
        document: Document,
        score: DocumentScore,
        query: SearchQuery,
        request: SearchRequest,
        band: BandMembership,
    ) -> DocumentSearchCandidate:
        return self._candidate(document, score, query, request, band)

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
    ) -> DocumentSearchCandidate:
        inner_join(on=document.id == score.document_id)
        inner_join(on=query.id == score.query_id)
        inner_join(on=request.query_id == query.id)
        left_join(on=band.user_id == request.user_id)
        where(score.score.is_not_null())
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
