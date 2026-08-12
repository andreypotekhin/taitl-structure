"""Staged similarity-search workflow."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.schemas.similarity import *
from examples.search.schemas.text import *
from examples.search.transforms.scoring.similarity import *
from examples.search.transforms.searching.search_similarity.adopt_lexical import *
from examples.search.transforms.searching.search_similarity.adopt_vector import *
from examples.search.transforms.searching.search_similarity.fusion import *
from examples.search.transforms.searching.search_similarity.rerank import *
from examples.search.transforms.vectorization import *
from structure import *


class SearchSimilarity(Transform):
    """Adopt lexical/vector candidates, fuse them, rerank them, and present the results."""

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_similarities = input(DocumentSimilarity)
    document_vector_embeddings = input(SimilarityDocumentVectorEmbedding)
    document_vector_index = input(DocumentVectorIndex)
    score_policy = input(ScorePolicy)
    vector_policy = input(VectorIndexPolicy)
    fusion_policy = input(SimilarityFusionPolicy)

    lexical = AdoptLexicalSimilarity(document_similarities=document_similarities)
    vectorized = VectorizeSimilarityDocumentQueries(
        queries=query,
        embeddings=document_vector_embeddings,
    )
    scored = ScoreDocumentVectors(
        policy=vector_policy,
        score_policy=score_policy,
        queries=vectorized.vector_queries,
        document_index=document_vector_index,
    )
    vector = AdoptVectorSimilarity(
        document_scores=scored.document_scores,
        query=query,
        documents=documents,
    )

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
