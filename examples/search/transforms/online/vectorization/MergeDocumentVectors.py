"""Merge cached and newly inferred document vectors for online search."""

from examples.search.schemas.indexing.vector import DocumentVectorEmbedding, DocumentVectorIndex, VectorIndexPolicy
from examples.search.transforms.lib.Vectors import Vectors
from structure import Transform, input, output, step
from structure.plugin.pyspark import drop_duplicates, param_join, union_all, where


class MergeDocumentVectors(Transform):
    cached = input(DocumentVectorIndex)
    inferred = input(DocumentVectorEmbedding, streaming=True)
    policy = input(VectorIndexPolicy)
    embeddings = output(DocumentVectorIndex)

    @step(input=[cached, inferred, policy], output=embeddings)
    def merge(
        self,
        cached_embedding: DocumentVectorIndex,
        inferred_embedding: DocumentVectorEmbedding,
        policy: VectorIndexPolicy,
    ) -> DocumentVectorIndex:
        merged = union_all(inferred_embedding)
        param_join(policy)
        where(Vectors.valid_embedding(merged, policy))
        drop_duplicates(merged.document_id)
        return DocumentVectorIndex.project(merged)
