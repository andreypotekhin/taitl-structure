"""Target-aware replacement for canonical online filter-gap selection."""

from examples.search.schemas.filtering import FilterQueryAvailability
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from examples.search.transforms.online.filtering.SelectGapQueries import SelectGapQueries as CanonicalSelectGapQueries
from structure import input, lane, step
from structure.plugin.pyspark import drop_duplicates, left_join, where


class SelectGapQueries(CanonicalSelectGapQueries):
    """Select missing-cache queries and always refresh target-scoped queries."""

    document_filter_targets = input(DocumentSearchTarget, streaming=True)

    @step(
        input=[
            CanonicalSelectGapQueries.queries,
            CanonicalSelectGapQueries.filter_availability,
            document_filter_targets,
        ],
        output=CanonicalSelectGapQueries.gap_queries,
    )
    def select_gap_queries(
        self,
        query: SearchQuery,
        availability: FilterQueryAvailability,
        target: DocumentSearchTarget,
    ) -> SearchQuery:
        left_join(availability, on=query.id == availability.query_id)
        left_join(target, on=query.id == target.query_id)
        where(availability.query_id.is_null() | target.query_id.is_not_null())
        drop_duplicates(query.id)
        return SearchQuery.project(query)
