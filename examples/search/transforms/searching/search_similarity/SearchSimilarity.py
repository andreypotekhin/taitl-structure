"""Staged similarity-search workflow."""

from examples.search.schemas.indexing.vector import DocumentVectorCandidate
from examples.search.schemas.similarity import (
    DocumentSimilarity,
    HybridIndexedSimilarDocument,
    SimilarityDocumentQuery,
    SimilarityFusionPolicy,
)
from examples.search.schemas.text import Document
from examples.search.transforms.searching.search_similarity.adopt_lexical import AdoptLexicalSimilarity
from examples.search.transforms.searching.search_similarity.adopt_vector import AdoptVectorSimilarity
from examples.search.transforms.searching.search_similarity.fusion import FuseSimilarity
from examples.search.transforms.searching.search_similarity.rerank import RerankSimilarity
from structure import Transform, input, output


class SearchSimilarity(Transform):
    """Adopt lexical/vector candidates, fuse them, rerank them, and present the results."""

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_similarities = input(DocumentSimilarity)
    document_vector_candidates = input(DocumentVectorCandidate)
    fusion_policy = input(SimilarityFusionPolicy)

    lexical = AdoptLexicalSimilarity(document_similarities=document_similarities)
    vector = AdoptVectorSimilarity(document_candidates=document_vector_candidates)

    fused = FuseSimilarity(
        policy=fusion_policy,
        document_lexical_candidates=lexical.document_candidates,
        document_vector_candidates=vector.adopted_document_candidates,
    )

    reranked = RerankSimilarity(
        query=query,
        documents=documents,
        document_candidates=fused.document_candidates,
        policy=fusion_policy,
    )

    similar_documents = output(HybridIndexedSimilarDocument, reranked.similar_documents)
