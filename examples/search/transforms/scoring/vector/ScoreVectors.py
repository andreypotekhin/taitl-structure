"""Score exact vector-index candidates with cosine similarity."""

from examples.search.schemas.indexing.vector import (
    DocumentVectorIndex,
    DocumentVectorQuery,
    DocumentVectorScore,
    ParagraphVectorIndex,
    ParagraphVectorQuery,
    ParagraphVectorScore,
    VectorIndexPolicy,
)
from examples.search.transforms.lib.Vectors import Vectors
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import coalesce, cross_join, param_join, require_all, where


class ScoreVectors(Transform):
    """Produce same-grain exact vector scores without driver collection."""

    policy = input(VectorIndexPolicy)
    document_queries = input(DocumentVectorQuery)
    document_index = input(DocumentVectorIndex)
    paragraph_queries = input(ParagraphVectorQuery)
    paragraph_index = input(ParagraphVectorIndex)
    valid_policy = lane(VectorIndexPolicy)
    document_scores = output(DocumentVectorScore)
    paragraph_scores = output(ParagraphVectorScore)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: VectorIndexPolicy) -> VectorIndexPolicy:
        validated = require_all(Vectors.valid_policy(policy))
        return VectorIndexPolicy.project(validated)

    @step(input=[document_queries, document_index, valid_policy], output=document_scores)
    def score_documents(
        self, query: DocumentVectorQuery, index: DocumentVectorIndex, policy: VectorIndexPolicy
    ) -> DocumentVectorScore:
        param_join(policy)
        cross_join(index, allow_cartesian=True)
        require_all(Vectors.valid_pair(query, index, policy))
        where(query.document_id != index.document_id)
        cosine = Vectors.cosine(query.vector, index.vector)
        return DocumentVectorScore(
            query_id=query.query_id,
            query_document_id=query.document_id,
            document_id=index.document_id,
            cosine_similarity=coalesce(cosine, 0.0),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
            vector_backend="exact_reference",
        )

    @step(input=[paragraph_queries, paragraph_index, valid_policy], output=paragraph_scores)
    def score_paragraphs(
        self, query: ParagraphVectorQuery, index: ParagraphVectorIndex, policy: VectorIndexPolicy
    ) -> ParagraphVectorScore:
        param_join(policy)
        cross_join(index, allow_cartesian=True)
        require_all(Vectors.valid_pair(query, index, policy))
        where(
            (query.document_id != index.document_id)
            | (query.section_id != index.section_id)
            | (query.paragraph_id != index.paragraph_id)
        )
        cosine = Vectors.cosine(query.vector, index.vector)
        return ParagraphVectorScore(
            query_id=query.query_id,
            query_document_id=query.document_id,
            query_section_id=query.section_id,
            query_paragraph_id=query.paragraph_id,
            document_id=index.document_id,
            section_id=index.section_id,
            paragraph_id=index.paragraph_id,
            cosine_similarity=coalesce(cosine, 0.0),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
            vector_backend="exact_reference",
        )
