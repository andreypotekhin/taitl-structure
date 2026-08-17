"""Score exact vector-index candidates with cosine similarity."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.transforms.lib.Vectors import *
from structure import *
from structure.plugin.pyspark import *


class ScoreVectors(Transform):
    """Produce same-grain exact vector scores without driver collection."""

    document_queries = input(DocumentVectorQuery)
    document_index = input(DocumentVectorIndex)
    paragraph_queries = input(ParagraphVectorQuery)
    paragraph_index = input(ParagraphVectorIndex)
    targets = input(DocumentSearchTarget, streaming=True)
    policy = input(VectorIndexPolicy)
    score_policy = input(ScorePolicy)
    valid_policy = lane(VectorIndexPolicy)
    document_scores = output(DocumentVectorScore)
    paragraph_scores = output(ParagraphVectorScore)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: VectorIndexPolicy) -> VectorIndexPolicy:
        validated = require_all(Vectors.valid_policy(policy))
        return VectorIndexPolicy.project(validated)

    @step(input=[document_queries, document_index, targets, valid_policy, score_policy], output=document_scores)
    def score_documents(
        self,
        query: DocumentVectorQuery,
        index: DocumentVectorIndex,
        target: DocumentSearchTarget,
        policy: VectorIndexPolicy,
        score_policy: ScorePolicy,
    ) -> DocumentVectorScore:
        param_join(policy)
        param_join(score_policy)
        cross_join(index, allow_cartesian=True)
        inner_join(target, on=(target.query_id == query.query_id) & (target.document_id == index.document_id))
        require_all(Vectors.valid_pair(query, index, policy))
        where(query.query_document_id.is_null() | (query.query_document_id != index.document_id))
        cosine = Vectors.cosine(query.vector, index.vector)
        return DocumentVectorScore(
            query_id=query.query_id,
            query_document_id=query.query_document_id,
            document_id=index.document_id,
            scope_id=target.scope_id,
            cosine_similarity=coalesce(cosine, 0.0),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
            vector_backend="exact_reference",
            scored_at=score_policy.scored_at,
        )

    @step(input=[paragraph_queries, paragraph_index, targets, valid_policy, score_policy], output=paragraph_scores)
    def score_paragraphs(
        self,
        query: ParagraphVectorQuery,
        index: ParagraphVectorIndex,
        target: DocumentSearchTarget,
        policy: VectorIndexPolicy,
        score_policy: ScorePolicy,
    ) -> ParagraphVectorScore:
        param_join(policy)
        param_join(score_policy)
        cross_join(index, allow_cartesian=True)
        inner_join(target, on=(target.query_id == query.query_id) & (target.document_id == index.document_id))
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
            scope_id=target.scope_id,
            cosine_similarity=coalesce(cosine, 0.0),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
            vector_backend="exact_reference",
            scored_at=score_policy.scored_at,
        )
