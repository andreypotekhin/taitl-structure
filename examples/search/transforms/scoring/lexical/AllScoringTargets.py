"""Expand queries to an unrestricted document scoring universe."""

from examples.search.schemas.indexing.lexical.index import DocumentTerm
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from structure import Transform, input, output, step
from structure.plugin.pyspark import cross_join, group_by, literal


class AllScoringTargets(Transform):
    """Provide an explicit all-document target relation for similarity scoring."""

    queries = input(SearchQuery)
    document_terms = input(DocumentTerm)
    targets = output(DocumentSearchTarget)

    @step(input=[queries, document_terms], output=targets)
    def expand(self, query: SearchQuery, term: DocumentTerm) -> DocumentSearchTarget:
        cross_join(term, allow_cartesian=True)
        group_by(query_id=query.id, document_id=term.document_id)
        return DocumentSearchTarget(
            query_id=query.id,
            document_id=term.document_id,
            scope_id=literal("all-scoring-targets-v1"),
        )
