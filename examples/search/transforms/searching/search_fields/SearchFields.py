"""Metadata search with automatic delegation of body clauses to SearchDocuments."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.fields import FieldSearchQuery, FieldSearchResult, FieldSearchTerm, FieldTerm
from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentTerm,
    ParagraphIndexSummary,
    ParagraphTerm,
    SectionIndexSummary,
    SectionTerm,
    SentenceIndexSummary,
    SentenceTerm,
)
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.scoring.overlap import DocumentOverlapScore
from examples.search.schemas.search import DocumentScore, ScorePolicy
from examples.search.schemas.text import Document
from examples.search.schemas.user import BandFallback, BandMembership
from examples.search.transforms.searching.search_fields.delegate import BuildDelegations
from examples.search.transforms.searching.search_fields.field_search import FieldSearch
from examples.search.transforms.searching.search_fields.publish import PublishFieldSearchResults
from examples.search.transforms.searching.search_fields.search_docs.SearchDocuments import SearchDocuments
from structure import Transform, input, output


class SearchFields(Transform):
    """Resolve metadata clauses and delegate body clauses to SearchDocuments."""

    queries = input(FieldSearchQuery, streaming=True)
    query_terms = input(FieldSearchTerm, streaming=True)
    field_terms = input(FieldTerm)
    requests = input(SearchRequest, streaming=True)
    documents = input(Document)
    document_scores = input(DocumentScore)
    streamed_documents = input(Document, streaming=True)
    streamed_document_scores = input(DocumentScore, streaming=True)
    document_overlap_scores = input(DocumentOverlapScore)
    document_filter_scores = input(DocumentFilterScore)
    document_terms = input(DocumentTerm)
    section_terms = input(SectionTerm)
    paragraph_terms = input(ParagraphTerm)
    sentence_terms = input(SentenceTerm)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    score_policy = input(ScorePolicy)
    band_memberships = input(BandMembership)
    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)

    resolved = FieldSearch(
        queries=queries,
        query_terms=query_terms,
        field_terms=field_terms,
    )
    delegation = BuildDelegations(
        queries=resolved.delegatable_queries,
        document_matches=resolved.document_matches,
        requests=requests,
    )
    delegated = SearchDocuments(
        queries=delegation.body_queries,
        documents=documents,
        document_scores=document_scores,
        streamed_documents=streamed_documents,
        streamed_document_scores=streamed_document_scores,
        document_overlap_scores=document_overlap_scores,
        document_filter_scores=document_filter_scores,
        document_filter_targets=delegation.document_filter_targets,
        document_terms=document_terms,
        section_terms=section_terms,
        paragraph_terms=paragraph_terms,
        sentence_terms=sentence_terms,
        document_summary=document_summary,
        section_summary=section_summary,
        paragraph_summary=paragraph_summary,
        sentence_summary=sentence_summary,
        score_policy=score_policy,
        requests=delegation.delegated_requests,
        band_memberships=band_memberships,
        query_document_signals=query_document_signals,
        document_popularity=document_popularity,
        band_fallbacks=band_fallbacks,
        policy=policy,
    )
    published = PublishFieldSearchResults(
        queries=queries,
        document_matches=resolved.document_matches,
        delegations=delegation.delegations,
        document_results=delegated.results,
    )
    results = output(FieldSearchResult, published.results)
