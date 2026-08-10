"""Target-aware replacement for canonical filtering."""

from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.filtering.Filtering import Filtering as CanonicalFiltering
from examples.search.transforms.searching.search_fields.filtering.FilterOverlap import FilterOverlap
from structure import input


class Filtering(CanonicalFiltering):
    """Create field-delegation filter artifacts with query-scoped targets."""

    document_filter_targets = input(DocumentSearchTarget, streaming=True)

    overlap = FilterOverlap(
        queries=CanonicalFiltering.queries,
        document_terms=CanonicalFiltering.document_terms,
        document_filter_targets=document_filter_targets,
        score_policy=CanonicalFiltering.score_policy,
    )

