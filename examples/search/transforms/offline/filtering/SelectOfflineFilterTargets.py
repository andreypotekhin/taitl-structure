"""Bound offline filter artifacts into the serving target universe."""

from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.lib.TargetScope import target_scope_id
from structure import Transform, input, output, step
from structure.plugin.pyspark import where


class SelectOfflineFilterTargets(Transform):
    """Create the same bounded target scope used by online filtering."""

    maximum_candidates = 10000

    document_filter_scores = input(DocumentFilterScore)
    targets = output(DocumentSearchTarget)

    @step(input=document_filter_scores, output=targets)
    def select_targets(self, document: DocumentFilterScore) -> DocumentSearchTarget:
        where(document.filter_rank <= self.maximum_candidates)
        return DocumentSearchTarget.project(document)(
            scope_id=target_scope_id(document.query_id, document.scored_at, self.maximum_candidates)
        )
