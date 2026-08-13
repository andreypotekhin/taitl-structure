"""Metadata search with automatic delegation of body clauses to SearchDocuments."""

from examples.search.schemas.clicks import *
from examples.search.schemas.fields import *
from examples.search.schemas.filtering import *
from examples.search.schemas.indexing.lexical.index import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.inference import *
from examples.search.schemas.relevance import *
from examples.search.schemas.scoring.overlap import *
from examples.search.schemas.search import *
from examples.search.schemas.text import *
from examples.search.schemas.user import *
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments
from examples.search.transforms.searching.search_fields.delegate import *
from examples.search.transforms.searching.search_fields.field_search import *
from examples.search.transforms.searching.search_fields.publish import *
from structure import *


class SearchFields(Transform):
    """Allow query clauses like field:value, send the rest of query to SearchDocuments."""

    queries = input(FieldSearchQuery, streaming=True)
    query_terms = input(FieldSearchTerm, streaming=True)
    field_terms = input(FieldTerm)
    requests = input(SearchRequest, streaming=True)
    documents = input(Document)
    document_scores = input(DocumentScore)
    document_vector_scores = input(DocumentVectorScore)
    streamed_documents = input(Document, streaming=True)
    streamed_document_scores = input(DocumentScore, streaming=True)
    document_overlap_scores = input(DocumentOverlapScore)
    document_filter_scores = input(DocumentFilterScore)
    document_terms = input(DocumentTerm)
    gap_policy = input(GapPolicy)
    document_summary = input(DocumentIndexSummary)
    score_policy = input(ScorePolicy)
    document_vector_embeddings = input(SearchQueryVectorEmbedding, streaming=True)
    document_vector_index = input(DocumentVectorIndex)
    vector_policy = input(VectorIndexPolicy)
    inference_policy = input(InferencePolicy)
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
        document_vector_scores=document_vector_scores,
        streamed_documents=streamed_documents,
        streamed_document_scores=streamed_document_scores,
        document_overlap_scores=document_overlap_scores,
        document_filter_scores=document_filter_scores,
        document_filter_targets=delegation.document_filter_targets,
        document_terms=document_terms,
        document_summary=document_summary,
        score_policy=score_policy,
        gap_policy=gap_policy,
        document_vector_embeddings=document_vector_embeddings,
        document_vector_index=document_vector_index,
        vector_policy=vector_policy,
        inference_policy=inference_policy,
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
