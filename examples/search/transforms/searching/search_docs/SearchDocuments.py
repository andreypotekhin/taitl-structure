"""Two-stage BM25 and implicit-feedback document search."""

from examples.search.transforms.searching.search_docs.RerankDocuments import RerankDocuments
from examples.search.transforms.searching.search_docs.RetrieveDocuments import RetrieveDocuments


class SearchDocuments(RetrieveDocuments, RerankDocuments):
    """Retrieve BM25 candidates, then rerank them with relevance signals."""
