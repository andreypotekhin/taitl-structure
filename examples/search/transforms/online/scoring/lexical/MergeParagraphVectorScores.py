"""Merge request-valid cached and online paragraph vector scores."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.indexing.vector import ParagraphVectorScore, VectorIndexPolicy
from examples.search.schemas.scoring.intermediate import ScoreQueryAvailability
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy, SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import datediff, drop_duplicates, inner_join, left_join, param_join, union_all, where


class MergeParagraphVectorScores(Transform):
    """Expose one authoritative paragraph vector-score relation for request ranking."""

    paragraph_vector_scores = input(ParagraphVectorScore)
    online_paragraph_vector_scores = input(ParagraphVectorScore, streaming=True)
    invalidated_queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    prefilter_targets = input(DocumentSearchTarget, streaming=True)
    score_policy = input(ScorePolicy)
    vector_policy = input(VectorIndexPolicy)
    invalidated = lane(ScoreQueryAvailability)
    cached_scores = lane(ParagraphVectorScore)
    valid_online_scores = lane(ParagraphVectorScore)
    scores = output(ParagraphVectorScore)

    @step(input=invalidated_queries, output=invalidated)
    def identify_invalidated_queries(self, query: SearchQuery) -> ScoreQueryAvailability:
        return ScoreQueryAvailability(query_id=query.id)

    @step(
        input=[paragraph_vector_scores, invalidated, requests, prefilter_targets, score_policy, vector_policy],
        output=cached_scores,
    )
    def select_cached_scores(
        self,
        score: ParagraphVectorScore,
        invalidated: ScoreQueryAvailability,
        request: SearchRequest,
        target: DocumentSearchTarget,
        policy: ScorePolicy,
        vector_policy: VectorIndexPolicy,
    ) -> ParagraphVectorScore:
        left_join(invalidated, on=score.query_id == invalidated.query_id)
        inner_join(request, on=request.query_id == score.query_id)
        inner_join(
            target,
            on=(target.query_id == score.query_id)
            & (target.document_id == score.document_id)
            & (target.scope_id == score.scope_id),
        )
        param_join(policy)
        param_join(vector_policy)
        age = datediff(request.requested_at, score.scored_at)
        where(
            invalidated.query_id.is_null()
            & (score.scored_at <= request.requested_at)
            & (score.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
            & (score.model_id == vector_policy.model_id)
            & (score.dimension == vector_policy.dimension)
            & (score.content_revision == vector_policy.content_revision)
            & (score.experiment_id == vector_policy.experiment_id)
            & score.experiment_id.null_safe_eq(request.experiment_id)
        )
        drop_duplicates(score.query_id, score.document_id, score.section_id, score.paragraph_id, score.experiment_id)
        return ParagraphVectorScore.project(score)

    @step(
        input=[online_paragraph_vector_scores, requests, prefilter_targets, score_policy, vector_policy],
        output=valid_online_scores,
    )
    def select_online_scores(
        self,
        score: ParagraphVectorScore,
        request: SearchRequest,
        target: DocumentSearchTarget,
        policy: ScorePolicy,
        vector_policy: VectorIndexPolicy,
    ) -> ParagraphVectorScore:
        inner_join(request, on=request.query_id == score.query_id)
        inner_join(
            target,
            on=(target.query_id == score.query_id)
            & (target.document_id == score.document_id)
            & (target.scope_id == score.scope_id),
        )
        param_join(policy)
        param_join(vector_policy)
        age = datediff(request.requested_at, score.scored_at)
        where(
            (score.scored_at <= request.requested_at)
            & (score.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
            & (score.model_id == vector_policy.model_id)
            & (score.dimension == vector_policy.dimension)
            & (score.content_revision == vector_policy.content_revision)
            & (score.experiment_id == vector_policy.experiment_id)
            & score.experiment_id.null_safe_eq(request.experiment_id)
        )
        drop_duplicates(score.query_id, score.document_id, score.section_id, score.paragraph_id, score.experiment_id)
        return ParagraphVectorScore.project(score)

    @step(input=[valid_online_scores, cached_scores], output=scores)
    def merge_scores(
        self,
        online: ParagraphVectorScore,
        cached: ParagraphVectorScore,
    ) -> ParagraphVectorScore:
        merged: ParagraphVectorScore = union_all(cached)
        drop_duplicates(merged.query_id, merged.document_id, merged.section_id, merged.paragraph_id, merged.experiment_id)
        return ParagraphVectorScore.project(merged)
