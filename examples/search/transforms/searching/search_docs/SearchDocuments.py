"""Two-stage BM25 and implicit-feedback document search."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.scoring.overlap import DocumentOverlapScore
from examples.search.schemas.search import DocumentScore, DocumentSearchResult, SearchQuery
from examples.search.schemas.text import Document
from examples.search.schemas.user import BandFallback, BandMembership
from examples.search.transforms.searching.search_docs.admit import RetrieveDocuments
from examples.search.transforms.searching.search_docs.overlap import OverlapDocuments
from examples.search.transforms.searching.search_docs.rerank import RerankDocuments
from structure import Transform, input, output, stage


class SearchDocuments(Transform):
    """Retrieve BM25 candidates, then rerank them with relevance signals."""

    queries = input(SearchQuery)
    documents = input(Document)
    document_scores = input(DocumentScore)
    streamed_documents = input(Document, streaming=True)
    streamed_document_scores = input(DocumentScore, streaming=True)
    document_overlap_scores = input(DocumentOverlapScore)
    requests = input(SearchRequest)
    band_memberships = input(BandMembership)
    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)
    retrieved = stage(
        RetrieveDocuments(
            queries=queries,
            documents=documents,
            document_scores=document_scores,
            streamed_documents=streamed_documents,
            streamed_document_scores=streamed_document_scores,
            requests=requests,
            band_memberships=band_memberships,
        )
    )
    overlapped = stage(
        OverlapDocuments(
            candidates=retrieved.candidates,
            document_overlap_scores=document_overlap_scores,
        )
    )
    reranked = stage(
        RerankDocuments(
            overlapped_candidates=overlapped.overlapped_candidates,
            query_document_signals=query_document_signals,
            document_popularity=document_popularity,
            band_fallbacks=band_fallbacks,
            policy=policy,
        )
    )
    results = output(DocumentSearchResult).from_(reranked.results)
