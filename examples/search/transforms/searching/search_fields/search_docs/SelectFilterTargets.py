"""Target-aware replacement for canonical filter-target selection."""

from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.searching.search_docs.filter import SelectFilterTargets as CanonicalSelectFilterTargets
from structure import input, step
from structure.plugin.pyspark import left_join, where


class SelectFilterTargets(CanonicalSelectFilterTargets):
    """Merge filter artifacts and enforce field-projected document targets."""

    document_filter_targets = input(DocumentSearchTarget, streaming=True)

    @step(
        input=[CanonicalSelectFilterTargets.merged_filter_scores, document_filter_targets],
        output=CanonicalSelectFilterTargets.targets,
    )
    def select_targets(
        self, document: DocumentFilterScore, target: DocumentSearchTarget
    ) -> DocumentSearchTarget:
        left_join(target, on=target.query_id == document.query_id)
        where(target.query_id.is_null() | (target.document_id == document.document_id))
        where(document.filter_rank <= self.maximum_candidates)
        return DocumentSearchTarget.project(document)

