"""Staged similarity-search workflow."""

from typing import Final

from examples.search.schemas.indexing.vector import DocumentVectorCandidate, VectorIndexPolicy
from examples.search.schemas.similarity import DocumentSimilarity, IndexedSimilarDocument, SimilarityDocumentQuery
from examples.search.schemas.text import Document
from examples.search.transforms.searching.search_similarity.adopt_lexical import AdoptLexicalSimilarity
from examples.search.transforms.searching.search_similarity.adopt_vector import AdoptVectorSimilarity
from examples.search.transforms.searching.search_similarity.fusion import FuseSimilarity
from examples.search.transforms.searching.search_similarity.rerank import RerankSimilarity
from structure import Transform, input, output


class SearchSimilarity(Transform):
    """Adopt lexical/vector candidates, fuse them, rerank them, and present the results."""

    maximum_results = RerankSimilarity.maximum_results

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_similarities = input(DocumentSimilarity)
    document_vector_candidates = input(DocumentVectorCandidate)
    vector_policy = input(VectorIndexPolicy)

    lexical = AdoptLexicalSimilarity(document_similarities=document_similarities)
    vector = AdoptVectorSimilarity(document_candidates=document_vector_candidates)
    fused = FuseSimilarity(
        policy=vector_policy,
        document_lexical_candidates=lexical.document_candidates,
        document_vector_candidates=vector.adopted_document_candidates,
    )

    reranked = RerankSimilarity(
        query=query,
        documents=documents,
        document_candidates=fused.document_candidates,
    )

    similar_documents = output(IndexedSimilarDocument, reranked.similar_documents)
