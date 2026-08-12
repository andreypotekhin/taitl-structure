"""Staged hybrid similarity workflow for paragraphs."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.schemas.similarity import *
from examples.search.schemas.text import *
from examples.search.transforms.scoring.similarity import *
from examples.search.transforms.searching.search_similarity.adopt_lexical import *
from examples.search.transforms.searching.search_similarity.adopt_vector import *
from examples.search.transforms.searching.search_similarity.fusion import *
from examples.search.transforms.searching.search_similarity.rerank import *
from structure import *


class SearchSimilarityParagraphs(Transform):
    """Fuse lexical/vector paragraph candidates and present ranked paragraphs."""

    query = input(Paragraph)
    paragraphs = input(Paragraph)
    paragraph_similarities = input(ParagraphSimilarity)
    paragraph_vector_queries = input(ParagraphVectorQuery)
    paragraph_vector_index = input(ParagraphVectorIndex)
    score_policy = input(ScorePolicy)
    vector_policy = input(VectorIndexPolicy)
    policy = input(SimilarityFusionPolicy)
    similar_paragraphs = output(HybridIndexedSimilarParagraph)

    lexical = AdoptLexicalParagraphs(paragraph_similarities=paragraph_similarities)
    scored = ScoreParagraphVectors(
        policy=vector_policy,
        score_policy=score_policy,
        queries=paragraph_vector_queries,
        paragraph_index=paragraph_vector_index,
    )
    vector = AdoptVectorParagraphs(
        paragraph_scores=scored.paragraph_scores,
        query=query,
        paragraphs=paragraphs,
    )
    fused = FuseSimilarityParagraphs(
        policy=policy,
        paragraph_lexical_candidates=lexical.paragraph_candidates,
        paragraph_vector_candidates=vector.adopted_paragraph_candidates,
    )

    reranked = RerankSimilarityParagraphs(
        query=query,
        paragraphs=paragraphs,
        paragraph_candidates=fused.paragraph_candidates,
        policy=policy,
    )

    similar_paragraphs = output(HybridIndexedSimilarParagraph, reranked.similar_paragraphs)
