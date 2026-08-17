"""Staged similarity-search workflow."""

from examples.search.algorithms.similarity.adapter import SimilarityCandidateAdapter
from examples.search.schemas.indexing.vector import DocumentVectorCandidate
from examples.search.schemas.search import *
from examples.search.schemas.similarity import *
from examples.search.schemas.text import *
from examples.search.transforms.scoring.similarity import *
from examples.search.transforms.searching.search_similarity.adopt import *
from examples.search.transforms.searching.search_similarity.fusion import *
from examples.search.transforms.searching.search_similarity.rerank import *
from examples.search.transforms.vectorization import *
from structure import *


class SearchSimilarity(Transform):
    """Adopt lexical/vector candidates, fuse them, rerank them, and present the results."""

    vector_adapter = parameter(SimilarityCandidateAdapter())

    query = input(SimilaritySearchQuery)
    documents = input(Document)
    document_similarities = input(DocumentSimilarity)
    document_vector_candidates = input(DocumentVectorCandidate)
    fusion_policy = input(SimilarityFusionPolicy)

    lexical = AdoptLexicalSimilarity(document_similarities=document_similarities)

    vector = AdoptVectorSimilarity(
        query=query,
        documents=documents,
        document_candidates=document_vector_candidates,
        adapter=vector_adapter,
    )

    fused = FuseSimilarity(
        document_lexical_candidates=lexical.document_candidates,
        document_vector_candidates=vector.adopted_document_candidates,
        policy=fusion_policy,
    )

    reranked = RerankSimilarity(
        query=query,
        documents=documents,
        document_candidates=fused.document_candidates,
        policy=fusion_policy,
    )

    similar_documents = output(IndexedSimilarDocument, reranked.similar_documents)
