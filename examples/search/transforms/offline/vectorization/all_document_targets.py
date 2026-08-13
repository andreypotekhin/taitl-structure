"""Create an offline target relation covering every document."""

from examples.search.schemas.search import DocumentSearchTarget
from examples.search.schemas.text import Document
from structure import Transform, input, output
from structure.plugin.pyspark import literal


class AllDocumentTargets(Transform):
    documents = input(Document)
    targets = output(DocumentSearchTarget)

    def target(self, document: Document) -> DocumentSearchTarget:
        return DocumentSearchTarget(
            query_id=literal("offline"),
            document_id=document.id,
            scope_id=literal("offline-vectorization-v1"),
        )
