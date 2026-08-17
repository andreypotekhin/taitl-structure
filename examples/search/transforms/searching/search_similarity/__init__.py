"""Document similarity-search stages and composition."""

from examples.search.transforms.searching.search_similarity.SearchSimilarity import SearchSimilarity
from examples.search.transforms.searching.search_similarity.adopt import AdoptLexicalSimilarity, AdoptVectorSimilarity
from examples.search.transforms.searching.search_similarity.candidates import ExactSimilarityCandidates
from examples.search.transforms.searching.search_similarity.fusion import FuseSimilarity
from examples.search.transforms.searching.search_similarity.rerank import RerankSimilarity

__all__ = [
    "SearchSimilarity",
    "AdoptLexicalSimilarity",
    "AdoptVectorSimilarity",
    "ExactSimilarityCandidates",
    "FuseSimilarity",
    "RerankSimilarity",
]
