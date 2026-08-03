"""Two-stage BM25 and implicit-feedback document search."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentIndexTerm,
    ParagraphIndexSummary,
    ParagraphIndexTerm,
    SectionIndexSummary,
    SectionIndexTerm,
    SentenceIndexSummary,
    SentenceIndexTerm,
)
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.scoring.overlap import DocumentOverlapScore
from examples.search.schemas.search import DocumentScore, DocumentSearchResult, ScorePolicy, SearchQuery
from examples.search.schemas.text import Document
from examples.search.schemas.user import BandFallback, BandMembership
from examples.search.transforms.searching.online.scoring import OnlineScoring
from examples.search.transforms.searching.search_docs.admit import RetrieveDocuments
from examples.search.transforms.searching.search_docs.overlap import OverlapDocuments
from examples.search.transforms.searching.search_docs.rerank import RerankDocuments
from structure import Transform, input, output


class SearchDocuments(Transform):
    """Retrieve BM25 candidates, then rerank them with relevance signals."""

    queries = input(SearchQuery, streaming=True)
    documents = input(Document)
    document_scores = input(DocumentScore)
    streamed_documents = input(Document, streaming=True)
    streamed_document_scores = input(DocumentScore, streaming=True)
    document_overlap_scores = input(DocumentOverlapScore)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    score_policy = input(ScorePolicy)
    requests = input(SearchRequest, streaming=True)
    band_memberships = input(BandMembership)
    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)

    scored = OnlineScoring(
        queries=queries,
        requests=requests,
        document_scores=document_scores,
        document_overlap_scores=document_overlap_scores,
        document_terms=document_terms,
        section_terms=section_terms,
        paragraph_terms=paragraph_terms,
        sentence_terms=sentence_terms,
        document_summary=document_summary,
        section_summary=section_summary,
        paragraph_summary=paragraph_summary,
        sentence_summary=sentence_summary,
        score_policy=score_policy,
    )

    retrieved = RetrieveDocuments(
        queries=queries,
        documents=documents,
        document_scores=document_scores,
        online_document_scores=scored.online_document_scores,
        streamed_documents=streamed_documents,
        streamed_document_scores=streamed_document_scores,
        online_streamed_document_scores=scored.online_streamed_document_scores,
        requests=requests,
        band_memberships=band_memberships,
        score_policy=score_policy,
    )

    overlapped = OverlapDocuments(
        candidates=retrieved.candidates,
        document_overlap_scores=document_overlap_scores,
        online_document_overlap_scores=scored.online_document_overlap_scores,
        requests=requests,
        score_policy=score_policy,
    )

    reranked = RerankDocuments(
        overlapped_candidates=overlapped.overlapped_candidates,
        query_document_signals=query_document_signals,
        document_popularity=document_popularity,
        band_fallbacks=band_fallbacks,
        policy=policy,
    )

    results = output(DocumentSearchResult, reranked.results)
