"""Select online target documents without compatible cached embeddings."""

from examples.search.schemas.indexing.vector import DocumentVectorIndex
from examples.search.schemas.inference import InferencePolicy
from examples.search.schemas.search import DocumentSearchTarget
from examples.search.schemas.text import Document
from examples.search.transforms.lib.Vectors import Vectors
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import drop_duplicates, inner_join, left_join, param_join, where


class SelectDocumentGaps(Transform):
    targets = input(DocumentSearchTarget, streaming=True)
    documents = input(Document)
    document_index = input(DocumentVectorIndex)
    policy = input(InferencePolicy)
    target_documents = lane(Document)
    gaps = output(Document)

    @step(input=[targets, documents], output=target_documents)
    def select_targets(self, target: DocumentSearchTarget, document: Document) -> Document:
        inner_join(document, on=target.document_id == document.id)
        return Document.project(document)

    @step(input=[target_documents, document_index, policy], output=gaps)
    def select_gaps(self, document: Document, index: DocumentVectorIndex, policy: InferencePolicy) -> Document:
        left_join(index, on=document.id == index.document_id)
        param_join(policy)
        where(index.document_id.is_null() | ~Vectors.valid_embedding(index, policy))
        drop_duplicates(document.id)
        return Document.project(document)
