"""Score similarity document queries against the validated document index."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.transforms.lib.Vectors import *
from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark import literal


class ScoreDocumentVectors(Transform):
    """Produce exact document vector scores for document-to-document similarity."""

    policy = input(VectorIndexPolicy)
    score_policy = input(ScorePolicy)
    queries = input(DocumentVectorQuery, streaming=True)
    document_index = input(DocumentVectorIndex)
    valid_policy = lane(VectorIndexPolicy)
    document_scores = output(DocumentVectorScore)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: VectorIndexPolicy) -> VectorIndexPolicy:
        validated = require_all(Vectors.valid_policy(policy))
        return VectorIndexPolicy.project(validated)

    @step(input=[queries, document_index, valid_policy, score_policy], output=document_scores)
    def score_documents(
        self,
        query: DocumentVectorQuery,
        index: DocumentVectorIndex,
        policy: VectorIndexPolicy,
        score_policy: ScorePolicy,
    ) -> DocumentVectorScore:
        param_join(policy)
        param_join(score_policy)
        cross_join(index, allow_cartesian=True)
        require_all(Vectors.valid_pair(query, index, policy))
        where(query.query_document_id.is_null() | (query.query_document_id != index.document_id))
        cosine = Vectors.cosine(query.vector, index.vector)
        return DocumentVectorScore(
            query_id=query.query_id,
            query_document_id=query.query_document_id,
            document_id=index.document_id,
            scope_id=literal("similarity-v1"),
            cosine_similarity=coalesce(cosine, 0.0),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
            vector_backend="exact_reference",
            scored_at=score_policy.scored_at,
        )
