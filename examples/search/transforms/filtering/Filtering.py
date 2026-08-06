"""Offline reusable-index document filtering composition."""

from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.indexing.lexical.index import DocumentIndexTerm
from examples.search.schemas.search import ScorePolicy, SearchQuery
from examples.search.transforms.filtering.FilterOverlap import FilterOverlap
from structure import Transform, input, output


class Filtering(Transform):
    """Create timestamped simple-overlap filter artifacts for selected queries."""

    queries = input(SearchQuery)
    document_terms = input(DocumentIndexTerm)
    score_policy = input(ScorePolicy)

    overlap = FilterOverlap(
        queries=queries,
        document_terms=document_terms,
        score_policy=score_policy,
    )

    document_filter_scores = output(DocumentFilterScore, overlap.document_filter_scores)


__all__ = ["Filtering"]
