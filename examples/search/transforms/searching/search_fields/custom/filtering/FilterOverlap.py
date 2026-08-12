"""Target-aware replacement for canonical simple-overlap filtering."""

from examples.search.schemas.filtering import DocumentFilterMatch
from examples.search.schemas.indexing.lexical.index import DocumentTerm
from examples.search.schemas.scoring.intermediate import QueryTerm
from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.filtering.FilterOverlap import FilterOverlap as CanonicalFilterOverlap
from structure import input, step
from structure.plugin.pyspark import count_distinct, group_by, inner_join, left_join, row_number, types, where
from structure.plugin.pyspark.dsl.expressions import literal


class FilterOverlap(CanonicalFilterOverlap):
    """Rank overlap matches while honoring delegated document targets."""

    document_filter_targets = input(DocumentSearchTarget, streaming=True)

    @step(
        input=[CanonicalFilterOverlap.expanded_query_terms, CanonicalFilterOverlap.document_terms, document_filter_targets],
        output=CanonicalFilterOverlap.matched_documents,
    )
    def match_documents(
        self, query: QueryTerm, term: DocumentTerm, target: DocumentSearchTarget
    ) -> DocumentFilterMatch:
        inner_join(on=term.term == query.token)
        left_join(target, on=target.query_id == query.query_id)
        where(target.query_id.is_null() | (target.document_id == term.document_id))
        zero_rank = literal(0).cast(types.long())
        group_by(query_id=query.query_id, document_id=term.document_id, filter_rank=zero_rank)
        return DocumentFilterMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            matched_terms=count_distinct(query.token),
            filter_rank=zero_rank,
        )

