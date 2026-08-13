"""Select query groups that need online score resolution."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.indexing.vector import DocumentVectorScore, ParagraphVectorScore, VectorIndexPolicy
from examples.search.schemas.scoring.intermediate import ScoreQueryAvailability
from examples.search.schemas.scoring.overlap import DocumentOverlapScore
from examples.search.schemas.search import DocumentScore, DocumentSearchTarget, GapPolicy, ScorePolicy, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import datediff, drop_duplicates, inner_join, left_join, param_join, where


class SelectGapQueries(Transform):
    """Select queries without fresh document and overlap scores."""

    queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    document_scores = input(DocumentScore)
    document_overlap_scores = input(DocumentOverlapScore)
    document_vector_scores = input(DocumentVectorScore)
    paragraph_vector_scores = input(ParagraphVectorScore, optional=True)
    prefilter_targets = input(DocumentSearchTarget, streaming=True)
    score_policy = input(ScorePolicy)
    gap_policy = input(GapPolicy)
    vector_policy = input(VectorIndexPolicy)

    document_availability = lane(ScoreQueryAvailability)
    overlap_availability = lane(ScoreQueryAvailability)
    vector_availability = lane(ScoreQueryAvailability)
    paragraph_vector_availability = lane(ScoreQueryAvailability)
    gap_queries = output(SearchQuery)

    @step(input=[prefilter_targets, document_scores, requests, score_policy, gap_policy], output=document_availability)
    def find_available_documents(
        self,
        target: DocumentSearchTarget,
        score: DocumentScore,
        request: SearchRequest,
        policy: ScorePolicy,
        gaps: GapPolicy,
    ) -> ScoreQueryAvailability:
        left_join(
            score,
            on=(score.query_id == target.query_id)
            & (score.document_id == target.document_id)
            & (score.scope_id == target.scope_id),
        )
        inner_join(request, on=request.query_id == target.query_id)
        param_join(policy)
        param_join(gaps)
        where(
            gaps.document_scores
            & (
                score.document_id.is_null()
                | ~self._is_fresh(score.scored_at, request.requested_at, policy.maximum_age_days, policy.effective_at)
            )
        )
        drop_duplicates(target.query_id)
        return ScoreQueryAvailability(query_id=target.query_id)

    @step(input=[prefilter_targets, document_overlap_scores, requests, score_policy, gap_policy], output=overlap_availability)
    def find_available_overlaps(
        self,
        target: DocumentSearchTarget,
        score: DocumentOverlapScore,
        request: SearchRequest,
        policy: ScorePolicy,
        gaps: GapPolicy,
    ) -> ScoreQueryAvailability:
        left_join(
            score,
            on=(score.query_id == target.query_id)
            & (score.document_id == target.document_id)
            & (score.scope_id == target.scope_id),
        )
        inner_join(request, on=request.query_id == target.query_id)
        param_join(policy)
        param_join(gaps)
        where(
            gaps.document_overlap_scores
            & (
                score.document_id.is_null()
                | ~self._is_fresh(score.scored_at, request.requested_at, policy.maximum_age_days, policy.effective_at)
            )
        )
        drop_duplicates(target.query_id)
        return ScoreQueryAvailability(query_id=target.query_id)

    @step(input=[prefilter_targets, document_vector_scores, requests, score_policy, vector_policy, gap_policy], output=vector_availability)
    def find_available_vectors(
        self,
        target: DocumentSearchTarget,
        score: DocumentVectorScore,
        request: SearchRequest,
        policy: ScorePolicy,
        vector_policy: VectorIndexPolicy,
        gaps: GapPolicy,
    ) -> ScoreQueryAvailability:
        left_join(
            score,
            on=(score.query_id == target.query_id)
            & (score.document_id == target.document_id)
            & (score.scope_id == target.scope_id),
        )
        inner_join(request, on=request.query_id == target.query_id)
        param_join(policy)
        param_join(vector_policy)
        param_join(gaps)
        where(
            gaps.document_vector_scores
            & (
                score.document_id.is_null()
                | ~(
                    self._is_fresh(score.scored_at, request.requested_at, policy.maximum_age_days, policy.effective_at)
                    & (score.model_id == vector_policy.model_id)
                    & (score.dimension == vector_policy.dimension)
                    & (score.content_revision == vector_policy.content_revision)
                    & (score.experiment_id == vector_policy.experiment_id)
                    & score.experiment_id.null_safe_eq(request.experiment_id)
                )
            )
        )
        drop_duplicates(target.query_id)
        return ScoreQueryAvailability(query_id=target.query_id)

    @step(input=[prefilter_targets, paragraph_vector_scores, requests, score_policy, vector_policy, gap_policy], output=paragraph_vector_availability)
    def find_available_paragraph_vectors(
        self,
        target: DocumentSearchTarget,
        score: ParagraphVectorScore,
        request: SearchRequest,
        policy: ScorePolicy,
        vector_policy: VectorIndexPolicy,
        gaps: GapPolicy,
    ) -> ScoreQueryAvailability:
        left_join(
            score,
            on=(score.query_id == target.query_id)
            & (score.document_id == target.document_id)
            & (score.scope_id == target.scope_id),
        )
        inner_join(request, on=request.query_id == target.query_id)
        param_join(policy)
        param_join(vector_policy)
        param_join(gaps)
        where(
            gaps.paragraph_vector_scores
            & (
                score.document_id.is_null()
                | ~(
                    self._is_fresh(score.scored_at, request.requested_at, policy.maximum_age_days, policy.effective_at)
                    & (score.model_id == vector_policy.model_id)
                    & (score.dimension == vector_policy.dimension)
                    & (score.content_revision == vector_policy.content_revision)
                    & (score.experiment_id == vector_policy.experiment_id)
                    & score.experiment_id.null_safe_eq(request.experiment_id)
                )
            )
        )
        drop_duplicates(target.query_id)
        return ScoreQueryAvailability(query_id=target.query_id)

    @step(
        input=[
            queries,
            document_availability,
            overlap_availability,
            vector_availability,
            paragraph_vector_availability,
        ],
        output=gap_queries,
    )
    def select_gap_queries(
        self,
        query: SearchQuery,
        document: ScoreQueryAvailability,
        overlap: ScoreQueryAvailability,
        vector: ScoreQueryAvailability,
        paragraph_vector: ScoreQueryAvailability,
    ) -> SearchQuery:
        left_join(document, on=query.id == document.query_id)
        left_join(overlap, on=query.id == overlap.query_id)
        left_join(vector, on=query.id == vector.query_id)
        left_join(paragraph_vector, on=query.id == paragraph_vector.query_id)
        where(
            document.query_id.is_not_null()
            | overlap.query_id.is_not_null()
            | vector.query_id.is_not_null()
            | paragraph_vector.query_id.is_not_null()
        )
        return SearchQuery.project(query)

    @staticmethod
    def _is_fresh(score_at: object, requested_at: object, maximum_age_days: object, effective_at: object) -> object:
        age = datediff(requested_at, score_at)
        return (score_at <= requested_at) & (score_at >= effective_at) & (age >= 0) & (age <= maximum_age_days)
