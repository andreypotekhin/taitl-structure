"""Staged hybrid similarity workflow for paragraphs."""

from examples.search.schemas.indexing.vector import ParagraphVectorCandidate
from examples.search.schemas.similarity import (
    HybridIndexedSimilarParagraph,
    ParagraphSimilarity,
    SimilarityFusionPolicy,
)
from examples.search.schemas.text import Paragraph
from examples.search.transforms.searching.search_similarity.adopt_lexical import AdoptLexicalParagraphs
from examples.search.transforms.searching.search_similarity.adopt_vector import AdoptVectorParagraphs
from examples.search.transforms.searching.search_similarity.fusion import FuseSimilarityParagraphs
from examples.search.transforms.searching.search_similarity.rerank import RerankSimilarityParagraphs
from structure import Transform, input, output


class SearchSimilarityParagraphs(Transform):
    """Fuse lexical/vector paragraph candidates and present ranked paragraphs."""

    query = input(Paragraph)
    paragraphs = input(Paragraph)
    paragraph_similarities = input(ParagraphSimilarity)
    paragraph_vector_candidates = input(ParagraphVectorCandidate)
    policy = input(SimilarityFusionPolicy)
    similar_paragraphs = output(HybridIndexedSimilarParagraph)

    lexical = AdoptLexicalParagraphs(paragraph_similarities=paragraph_similarities)
    vector = AdoptVectorParagraphs(paragraph_candidates=paragraph_vector_candidates)
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
