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
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    arr_aggregate,
    arr_filter,
    count,
    cross_join,
    exactly_one,
    group_by,
    isnan,
    require_all,
    size,
    sqrt,
)

_MAX_FINITE_DOUBLE = 1.7976931348623157e308


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
        validated = require_all(
            (policy.model_id != "")
            & (policy.dimension > 0)
            & (policy.content_revision != "")
            & (policy.experiment_id != "")
            & (policy.maximum_candidates > 0)
            & (policy.rrf_k > 0)
        )
        return VectorIndexPolicy.project(validated)

    @step(input=[document_embeddings, valid_policy], output=document_index)
    def index_documents(
        self, embedding: DocumentVectorEmbedding, policy: VectorIndexPolicy
    ) -> DocumentVectorIndex:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        require_all(self._valid_embedding(embedding, policy))
        return DocumentVectorIndex.project(embedding)

    @step(input=[paragraph_embeddings, valid_policy], output=paragraph_index)
    def index_paragraphs(
        self, embedding: ParagraphVectorEmbedding, policy: VectorIndexPolicy
    ) -> ParagraphVectorIndex:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        require_all(self._valid_embedding(embedding, policy))
        return ParagraphVectorIndex.project(embedding)

    @step(input=[document_index, valid_policy], output=document_summary)
    def summarize_documents(
        self, embedding: DocumentVectorIndex, policy: VectorIndexPolicy
    ) -> DocumentVectorIndexSummary:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
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
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
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

    @staticmethod
    def _valid_embedding(embedding, policy):
        finite_values = arr_filter(
            embedding.vector,
            lambda value: isnan(value) | (value > _MAX_FINITE_DOUBLE) | (value < -_MAX_FINITE_DOUBLE),
        )
        norm = sqrt(arr_aggregate(embedding.vector, 0.0, lambda total, value: total + value * value))
        return (
            (embedding.model_id == policy.model_id)
            & (embedding.dimension == policy.dimension)
            & (embedding.content_revision == policy.content_revision)
            & (embedding.experiment_id == policy.experiment_id)
            & (embedding.dimension == size(embedding.vector))
            & (size(embedding.vector) > 0)
            & (size(finite_values) == 0)
            & (norm > 0.0)
        )
