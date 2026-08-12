"""Score similarity paragraph queries against the validated paragraph index."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from examples.search.transforms.lib.Vectors import *
from structure import *
from structure.plugin.pyspark import *


class ScoreParagraphVectors(Transform):
    """Produce exact paragraph vector scores for document-to-document similarity."""

    policy = input(VectorIndexPolicy)
    score_policy = input(ScorePolicy)
    queries = input(ParagraphVectorQuery)
    paragraph_index = input(ParagraphVectorIndex)
    valid_policy = lane(VectorIndexPolicy)
    paragraph_scores = output(ParagraphVectorScore)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: VectorIndexPolicy) -> VectorIndexPolicy:
        validated = require_all(Vectors.valid_policy(policy))
        return VectorIndexPolicy.project(validated)

    @step(input=[queries, paragraph_index, valid_policy, score_policy], output=paragraph_scores)
    def score_paragraphs(
        self,
        query: ParagraphVectorQuery,
        index: ParagraphVectorIndex,
        policy: VectorIndexPolicy,
        score_policy: ScorePolicy,
    ) -> ParagraphVectorScore:
        param_join(policy)
        param_join(score_policy)
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
            scored_at=score_policy.scored_at,
        )
