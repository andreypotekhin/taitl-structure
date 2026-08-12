"""Two-stage BM25 and implicit-feedback document search."""

from examples.search.schemas.clicks import *
from examples.search.schemas.filtering import *
from examples.search.schemas.indexing.lexical.index import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.relevance import *
from examples.search.schemas.scoring.overlap import *
from examples.search.schemas.search import *
from examples.search.schemas.text import *
from examples.search.schemas.user import *
from examples.search.transforms.online.filtering import *
from examples.search.transforms.online.ranking import *
from examples.search.transforms.online.scoring.lexical import *
from examples.search.transforms.searching.search_docs.filter import *
from examples.search.transforms.searching.search_docs.fusion import *
from examples.search.transforms.searching.search_docs.rerank import *
from examples.search.transforms.searching.search_docs.retrieve import *
from examples.search.transforms.vectorization import *
from structure import *


class SearchDocuments(Transform):
    """Filter, retrieve, fuse, rerank, and return document search results."""

    queries = input(SearchQuery, streaming=True)
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
    document_vector_embeddings = input(SearchQueryVectorEmbedding, streaming=True)
    document_vector_index = input(DocumentVectorIndex)
    paragraph_vector_queries = input(ParagraphVectorQuery)
    paragraph_vector_index = input(ParagraphVectorIndex)
    vector_policy = input(VectorIndexPolicy)
    requests = input(SearchRequest, streaming=True)
    band_memberships = input(BandMembership)
    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)

    vector_queries = VectorizeSearchQueries(
        queries=queries,
        embeddings=document_vector_embeddings,
    )

    filtered = OnlineFiltering(
        queries=queries,
        requests=requests,
        document_filter_scores=document_filter_scores,
        document_terms=document_terms,
        score_policy=score_policy,
    )

    selected = SelectFilterTargets(
        document_filter_scores=document_filter_scores,
        online_document_filter_scores=filtered.online_document_filter_scores,
        requests=requests,
        score_policy=score_policy,
    )

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
        document_vector_queries=vector_queries.vector_queries,
        document_vector_index=document_vector_index,
        paragraph_vector_queries=paragraph_vector_queries,
        paragraph_vector_index=paragraph_vector_index,
        vector_policy=vector_policy,
    )

    ranked = OnlineRanking(
        policy=vector_policy,
        document_scores=scored.online_document_vector_scores,
        paragraph_scores=scored.online_paragraph_vector_scores,
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
        document_vector_candidates=ranked.online_document_vector_candidates,
        vector_policy=vector_policy,
        prefilter_targets=selected.targets,
    )

    fused = FuseDocumentCandidates(
        lexical_candidates=retrieved.candidates,
        vector_candidates=retrieved.vector_search_candidates,
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
