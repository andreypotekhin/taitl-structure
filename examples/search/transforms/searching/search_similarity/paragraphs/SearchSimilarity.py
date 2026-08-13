"""Staged hybrid similarity workflow for paragraphs."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.schemas.similarity import *
from examples.search.schemas.text import *
from examples.search.transforms.scoring.similarity import *
from examples.search.transforms.searching.search_similarity.paragraphs.adopt_lexical import *
from examples.search.transforms.searching.search_similarity.paragraphs.adopt_vector import *
from examples.search.transforms.searching.search_similarity.paragraphs.fusion import *
from examples.search.transforms.searching.search_similarity.paragraphs.rerank import *
from structure import *


class SearchSimilarity(Transform):
    """Fuse lexical/vector paragraph candidates and present ranked paragraphs."""

    query = input(Paragraph)
    paragraphs = input(Paragraph)
    paragraph_similarities = input(ParagraphSimilarity)
    paragraph_vector_queries = input(ParagraphVectorQuery)
    paragraph_vector_index = input(ParagraphVectorIndex)
    score_policy = input(ScorePolicy)
    vector_policy = input(VectorIndexPolicy)
    policy = input(SimilarityFusionPolicy)
    similar_paragraphs = output(IndexedSimilarParagraph)

    lexical = AdoptLexicalSimilarity(paragraph_similarities=paragraph_similarities)

    scored = ScoreParagraphVectors(
        score_policy=score_policy,
        queries=paragraph_vector_queries,
        paragraph_index=paragraph_vector_index,
        policy=vector_policy,
    )

    vector = AdoptVectorSimilarity(
        query=query,
        paragraphs=paragraphs,
        paragraph_scores=scored.paragraph_scores,
    )

    fused = FuseSimilarity(
        paragraph_lexical_candidates=lexical.paragraph_candidates,
        paragraph_vector_candidates=vector.adopted_paragraph_candidates,
        policy=policy,
    )

    reranked = RerankSimilarity(
        query=query,
        paragraphs=paragraphs,
        paragraph_candidates=fused.paragraph_candidates,
        policy=policy,
    )

    similar_paragraphs = output(IndexedSimilarParagraph, reranked.similar_paragraphs)
