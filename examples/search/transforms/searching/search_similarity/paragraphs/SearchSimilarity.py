"""Staged hybrid similarity workflow for paragraphs."""

from examples.search.algorithms.similarity.adapter import SimilarityCandidateAdapter
from examples.search.schemas.indexing.vector import ParagraphVectorCandidate
from examples.search.schemas.search import *
from examples.search.schemas.similarity import *
from examples.search.schemas.text import *
from examples.search.transforms.searching.search_similarity.paragraphs.adopt import *
from examples.search.transforms.searching.search_similarity.paragraphs.fusion import *
from examples.search.transforms.searching.search_similarity.paragraphs.rerank import *
from structure import *


class SearchSimilarity(Transform):
    """Fuse lexical/vector paragraph candidates and present ranked paragraphs."""

    query = input(Paragraph)
    paragraphs = input(Paragraph)
    paragraph_similarities = input(ParagraphSimilarity)
    paragraph_vector_candidates = input(ParagraphVectorCandidate)
    policy = input(SimilarityFusionPolicy)
    vector_adapter = parameter(SimilarityCandidateAdapter())
    similar_paragraphs = output(IndexedSimilarParagraph)

    lexical = AdoptLexicalSimilarity(paragraph_similarities=paragraph_similarities)

    vector = AdoptVectorSimilarity(
        query=query,
        paragraphs=paragraphs,
        paragraph_candidates=paragraph_vector_candidates,
        adapter=vector_adapter,
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
