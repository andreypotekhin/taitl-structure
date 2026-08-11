"""Build validated exact vector-index artifacts."""

from examples.search.schemas.indexing.vector import (
    DocumentVectorEmbedding,
    DocumentVectorIndex,
    DocumentVectorIndexSummary,
    ParagraphVectorEmbedding,
    ParagraphVectorIndex,
    ParagraphVectorIndexSummary,
    VectorIndexPolicy,
)
from examples.search.transforms.lib.Vectors import Vectors
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import count, group_by, param_join, require_all


class VectorIndex(Transform):
    """Validate caller-produced embeddings and retain them as an exact index."""

    policy = input(VectorIndexPolicy)
    document_embeddings = input(DocumentVectorEmbedding)
    paragraph_embeddings = input(ParagraphVectorEmbedding)
    valid_policy = lane(VectorIndexPolicy)
    document_index = output(DocumentVectorIndex)
    paragraph_index = output(ParagraphVectorIndex)
    document_summary = output(DocumentVectorIndexSummary)
    paragraph_summary = output(ParagraphVectorIndexSummary)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: VectorIndexPolicy) -> VectorIndexPolicy:
        validated = require_all(Vectors.valid_policy(policy))
        return VectorIndexPolicy.project(validated)

    @step(input=[document_embeddings, valid_policy], output=document_index)
    def index_documents(
        self, embedding: DocumentVectorEmbedding, policy: VectorIndexPolicy
    ) -> DocumentVectorIndex:
        param_join(policy)
        require_all(Vectors.valid_embedding(embedding, policy))
        return DocumentVectorIndex.project(embedding)

    @step(input=[paragraph_embeddings, valid_policy], output=paragraph_index)
    def index_paragraphs(
        self, embedding: ParagraphVectorEmbedding, policy: VectorIndexPolicy
    ) -> ParagraphVectorIndex:
        param_join(policy)
        require_all(Vectors.valid_embedding(embedding, policy))
        return ParagraphVectorIndex.project(embedding)

    @step(input=[document_index, valid_policy], output=document_summary)
    def summarize_documents(
        self, embedding: DocumentVectorIndex, policy: VectorIndexPolicy
    ) -> DocumentVectorIndexSummary:
        param_join(policy)
        group_by(
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
        )
        return DocumentVectorIndexSummary(
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
            target_count=count(),
        )

    @step(input=[paragraph_index, valid_policy], output=paragraph_summary)
    def summarize_paragraphs(
        self, embedding: ParagraphVectorIndex, policy: VectorIndexPolicy
    ) -> ParagraphVectorIndexSummary:
        param_join(policy)
        group_by(
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
        )
        return ParagraphVectorIndexSummary(
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
            target_count=count(),
        )
