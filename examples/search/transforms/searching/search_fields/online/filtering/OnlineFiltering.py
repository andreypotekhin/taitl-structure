"""Target-aware replacement for canonical online filtering."""

from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.searching.online.filtering.OnlineFiltering import (
    OnlineFiltering as CanonicalOnlineFiltering,
)
from examples.search.transforms.searching.search_fields.filtering.Filtering import Filtering
from examples.search.transforms.searching.search_fields.online.filtering.SelectGapQueries import SelectGapQueries
from structure import input


class OnlineFiltering(CanonicalOnlineFiltering):
    """Refresh target-scoped delegated filters while retaining ordinary cache gaps."""

    document_filter_targets = input(DocumentSearchTarget, streaming=True)

    gap = SelectGapQueries(
        queries=CanonicalOnlineFiltering.queries,
        requests=CanonicalOnlineFiltering.requests,
        document_filter_scores=CanonicalOnlineFiltering.document_filter_scores,
        document_filter_targets=document_filter_targets,
        score_policy=CanonicalOnlineFiltering.score_policy,
    )

    filtering = Filtering(
        queries=gap.gap_queries,
        document_terms=CanonicalOnlineFiltering.document_terms,
        document_filter_targets=document_filter_targets,
        score_policy=CanonicalOnlineFiltering.score_policy,
    )

