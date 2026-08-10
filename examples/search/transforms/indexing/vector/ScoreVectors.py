"""Score exact vector-index candidates with cosine similarity."""

from examples.search.schemas.indexing.vector import (
    DocumentVectorCandidate,
    DocumentVectorIndex,
    DocumentVectorQuery,
    DocumentVectorScore,
    ParagraphVectorCandidate,
    ParagraphVectorIndex,
    ParagraphVectorQuery,
    ParagraphVectorScore,
    VectorIndexPolicy,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    arr_aggregate,
    arr_filter,
    arr_zip_with,
    coalesce,
    cross_join,
    exactly_one,
    isnan,
    require_all,
    row_number,
    size,
    sqrt,
    where,
)

_MAX_FINITE_DOUBLE = 1.7976931348623157e308


class ScoreVectors(Transform):
    """Produce deterministic same-grain exact vector candidates without driver collection."""

    policy = input(VectorIndexPolicy)
    document_queries = input(DocumentVectorQuery)
    document_index = input(DocumentVectorIndex)
    paragraph_queries = input(ParagraphVectorQuery)
    paragraph_index = input(ParagraphVectorIndex)
    valid_policy = lane(VectorIndexPolicy)
    document_scores = lane(DocumentVectorScore)
    paragraph_scores = lane(ParagraphVectorScore)
    ranked_document_candidates = lane(DocumentVectorCandidate)
    ranked_paragraph_candidates = lane(ParagraphVectorCandidate)
    document_candidates = output(DocumentVectorCandidate)
    paragraph_candidates = output(ParagraphVectorCandidate)

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

    @step(input=[document_queries, document_index, valid_policy], output=document_scores)
    def score_documents(
        self, query: DocumentVectorQuery, index: DocumentVectorIndex, policy: VectorIndexPolicy
    ) -> DocumentVectorScore:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        cross_join(index, allow_cartesian=True)
        require_all(self._valid_pair(query, index, policy))
        where(query.document_id != index.document_id)
        cosine = self._cosine(query.vector, index.vector)
        return DocumentVectorScore(
            query_id=query.query_id,
            query_document_id=query.document_id,
            document_id=index.document_id,
            cosine_similarity=coalesce(cosine, 0.0),
            model_id=policy.model_id,
            dimension=policy.dimension,
            content_revision=policy.content_revision,
            experiment_id=policy.experiment_id,
        )

    @step(input=[document_scores, valid_policy], output=ranked_document_candidates)
    def rank_documents(
        self, score: DocumentVectorScore, policy: VectorIndexPolicy
    ) -> DocumentVectorCandidate:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        return DocumentVectorCandidate.project(score)(
            rank=row_number(
                partition_by=score.query_id,
                order_by=(score.cosine_similarity.desc_nulls_last(), score.document_id.asc_nulls_first()),
            )
        )

    @step(input=[ranked_document_candidates, valid_policy], output=document_candidates)
    def publish_documents(
        self, candidate: DocumentVectorCandidate, policy: VectorIndexPolicy
    ) -> DocumentVectorCandidate:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        where(candidate.rank <= policy.maximum_candidates)
        return DocumentVectorCandidate.project(candidate)

    @step(input=[paragraph_queries, paragraph_index, valid_policy], output=paragraph_scores)
    def score_paragraphs(
        self, query: ParagraphVectorQuery, index: ParagraphVectorIndex, policy: VectorIndexPolicy
    ) -> ParagraphVectorScore:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        cross_join(index, allow_cartesian=True)
        require_all(self._valid_pair(query, index, policy))
        where(
            (query.document_id != index.document_id)
            | (query.section_id != index.section_id)
            | (query.paragraph_id != index.paragraph_id)
        )
        cosine = self._cosine(query.vector, index.vector)
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
        )

    @step(input=[paragraph_scores, valid_policy], output=ranked_paragraph_candidates)
    def rank_paragraphs(
        self, score: ParagraphVectorScore, policy: VectorIndexPolicy
    ) -> ParagraphVectorCandidate:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        return ParagraphVectorCandidate.project(score)(
            rank=row_number(
                partition_by=score.query_id,
                order_by=(
                    score.cosine_similarity.desc_nulls_last(),
                    score.document_id.asc_nulls_first(),
                    score.section_id.asc_nulls_first(),
                    score.paragraph_id.asc_nulls_first(),
                ),
            )
        )

    @step(input=[ranked_paragraph_candidates, valid_policy], output=paragraph_candidates)
    def publish_paragraphs(
        self, candidate: ParagraphVectorCandidate, policy: VectorIndexPolicy
    ) -> ParagraphVectorCandidate:
        exactly_one(policy)
        cross_join(policy, allow_cartesian=True)
        where(candidate.rank <= policy.maximum_candidates)
        return ParagraphVectorCandidate.project(candidate)

    @staticmethod
    def _valid_pair(query, index, policy):
        return (
            (query.model_id == policy.model_id)
            & (index.model_id == policy.model_id)
            & (query.dimension == policy.dimension)
            & (index.dimension == policy.dimension)
            & (query.content_revision == policy.content_revision)
            & (index.content_revision == policy.content_revision)
            & (query.experiment_id == policy.experiment_id)
            & (index.experiment_id == policy.experiment_id)
            & ScoreVectors._valid_vector(query.vector, query.dimension)
            & ScoreVectors._valid_vector(index.vector, index.dimension)
        )

    @staticmethod
    def _valid_vector(vector, dimension):
        finite_values = arr_filter(
            vector,
            lambda value: isnan(value) | (value > _MAX_FINITE_DOUBLE) | (value < -_MAX_FINITE_DOUBLE),
        )
        norm = sqrt(arr_aggregate(vector, 0.0, lambda total, value: total + value * value))
        return (dimension == size(vector)) & (size(vector) > 0) & (size(finite_values) == 0) & (norm > 0.0)

    @staticmethod
    def _cosine(left, right):
        products = arr_zip_with(left, right, lambda left_value, right_value: left_value * right_value)
        dot = arr_aggregate(products, 0.0, lambda total, value: total + value)
        left_norm = sqrt(arr_aggregate(left, 0.0, lambda total, value: total + value * value))
        right_norm = sqrt(arr_aggregate(right, 0.0, lambda total, value: total + value * value))
        return dot / (left_norm * right_norm)
