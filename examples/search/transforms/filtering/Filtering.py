"""Offline reusable-index document filtering composition."""

from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.indexing.lexical.index import DocumentTerm
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy, SearchQuery
from examples.search.transforms.filtering.FilterOverlap import FilterOverlap
from structure import Transform, input, output


class Filtering(Transform):
    """Create timestamped simple-overlap filter artifacts for selected queries."""

    queries = input(SearchQuery, streaming=True)
    document_terms = input(DocumentTerm)
    document_filter_targets = input(DocumentSearchTarget, streaming=True)
    score_policy = input(ScorePolicy)

    overlap = FilterOverlap(
        queries=queries,
        document_terms=document_terms,
        document_filter_targets=document_filter_targets,
        score_policy=score_policy,
    )

    document_filter_scores = output(DocumentFilterScore, overlap.document_filter_scores)


__all__ = ["Filtering"]
