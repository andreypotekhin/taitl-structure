"""Expand queries to an unrestricted document scoring universe."""

from examples.search.schemas.indexing.lexical.index import DocumentTerm
from examples.search.schemas.scoring.intermediate import ScoringTargetGroup
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import count, cross_join, group_by, literal


class AllScoringTargets(Transform):
    """Provide an explicit all-document target relation for similarity scoring."""

    queries = input(SearchQuery)
    document_terms = input(DocumentTerm)
    grouped_targets = lane(ScoringTargetGroup)
    targets = output(DocumentSearchTarget)

    @step(input=[queries, document_terms], output=grouped_targets)
    def expand(self, query: SearchQuery, term: DocumentTerm) -> ScoringTargetGroup:
        cross_join(term, allow_cartesian=True)
        scope_id = literal("all-scoring-targets-v1")
        group_by(query_id=query.id, document_id=term.document_id, scope_id=scope_id)
        return ScoringTargetGroup.project(query, term)(
            query_id=query.id,
            scope_id=scope_id,
            row_count=count(),
        )

    @step(input=grouped_targets, output=targets)
    def publish(self, target: ScoringTargetGroup) -> DocumentSearchTarget:
        return DocumentSearchTarget.project(target)
