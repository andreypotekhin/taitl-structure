"""Target-aware replacement for canonical online filtering."""

from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.online.filtering.OnlineFiltering import OnlineFiltering as CanonicalOnlineFiltering
from examples.search.transforms.searching.search_fields.custom.filtering.Filtering import Filtering
from examples.search.transforms.searching.search_fields.custom.online.filtering.SelectGapQueries import SelectGapQueries
from examples.search.transforms.searching.search_fields.custom.search_docs.SelectFilterTargets import (
    SelectFilterTargets,
)
from structure import input, output


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

    selected = SelectFilterTargets(
        document_filter_scores=CanonicalOnlineFiltering.document_filter_scores,
        online_document_filter_scores=filtering.document_filter_scores,
        document_filter_targets=document_filter_targets,
        requests=CanonicalOnlineFiltering.requests,
        score_policy=CanonicalOnlineFiltering.score_policy,
    )

    online_document_filter_scores = output(DocumentFilterScore, filtering.document_filter_scores)
    targets = output(DocumentSearchTarget, selected.targets)
