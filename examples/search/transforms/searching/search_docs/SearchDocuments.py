"""Hybrid BM25, vector and implicit-feedback document search."""

from examples.search.schemas.clicks import *
from examples.search.schemas.filtering import *
from examples.search.schemas.indexing.lexical.index import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.inference import *
from examples.search.schemas.relevance import *
from examples.search.schemas.scoring.overlap import *
from examples.search.schemas.search import *
from examples.search.schemas.text import *
from examples.search.schemas.user import *
from examples.search.transforms.online.filtering import *
from examples.search.transforms.online.scoring.lexical import *
from examples.search.transforms.online.vectorization import *
from examples.search.transforms.searching.search_docs.fuse import *
from examples.search.transforms.searching.search_docs.rerank import *
from examples.search.transforms.searching.search_docs.retrieve import *
from examples.search.transforms.vectorization import *
from structure import *


class SearchDocuments(Transform):
    """Full-text document search."""

    queries = input(SearchQuery, streaming=True)
    documents = input(Document)
    document_scores = input(DocumentScore)
    document_vector_scores = input(DocumentVectorScore)
    streamed_documents = input(Document, streaming=True)
    streamed_document_scores = input(DocumentScore, streaming=True)
    document_filter_targets = input(DocumentSearchTarget, streaming=True, optional=True)
    document_overlap_scores = input(DocumentOverlapScore)
    document_filter_scores = input(DocumentFilterScore)
    document_terms = input(DocumentTerm)
    document_summary = input(DocumentIndexSummary)
    score_policy = input(ScorePolicy)
    gap_policy = input(GapPolicy)
    document_vector_embeddings = input(SearchQueryVectorEmbedding, streaming=True)
    document_vector_index = input(DocumentVectorIndex)
    vector_policy = input(VectorIndexPolicy)
    inference_policy = input(InferencePolicy)
    requests = input(SearchRequest, streaming=True)
    band_memberships = input(BandMembership)
    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)

    filtered = OnlineFiltering(
        queries=queries,
        requests=requests,
        document_filter_scores=document_filter_scores,
        document_terms=document_terms,
        document_filter_targets=document_filter_targets,
        score_policy=score_policy,
    )

    vectorized = OnlineVectorization(
        queries=queries,
        documents=documents,
        cached_query_embeddings=document_vector_embeddings,
        document_vector_index=document_vector_index,
        document_targets=filtered.targets,
        inference_policy=inference_policy,
        vector_policy=vector_policy,
    )

    scored = OnlineScoring(
        queries=queries,
        requests=requests,
        streamed_document_scores=streamed_document_scores,
        cached_document_scores=document_scores,
        cached_document_vector_scores=document_vector_scores,
        cached_document_overlap_scores=document_overlap_scores,
        prefilter_targets=filtered.targets,
        document_terms=document_terms,
        document_summary=document_summary,
        score_policy=score_policy,
        gap_policy=gap_policy,
        document_vector_queries=vectorized.vector_queries,
        document_vector_index=vectorized.document_embeddings,
        vector_policy=vector_policy,
    )

    retrieved = RetrieveDocuments(
        queries=queries,
        documents=documents,
        online_document_scores=scored.document_scores,
        streamed_documents=streamed_documents,
        requests=requests,
        band_memberships=band_memberships,
        score_policy=score_policy,
        document_vector_scores=scored.document_vector_scores,
        vector_policy=vector_policy,
        prefilter_targets=filtered.targets,
    )

    fused = FuseDocuments(
        lexical_candidates=retrieved.candidates,
        vector_candidates=retrieved.vector_candidates,
        policy=vector_policy,
    )

    reranked = RerankDocuments(
        candidates=fused.candidates,
        query_document_signals=query_document_signals,
        document_popularity=document_popularity,
        band_fallbacks=band_fallbacks,
        policy=policy,
    )

    results = output(DocumentSearchResult, reranked.results)
