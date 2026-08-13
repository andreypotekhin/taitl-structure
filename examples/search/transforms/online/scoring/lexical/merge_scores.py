"""Merge stored, streamed, and newly calculated document scores."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.search import DocumentScore, DocumentSearchTarget, ScorePolicy
from structure import Transform, input, output, step
from structure.plugin.pyspark import datediff, drop_duplicates, inner_join, param_join, union_all, where


class MergeDocumentScores(Transform):
    """Expose one request-valid document-score lane for retrieval."""

    document_scores = input(DocumentScore)
    streamed_document_scores = input(DocumentScore, streaming=True)
    online_document_scores = input(DocumentScore, streaming=True)
    requests = input(SearchRequest, streaming=True)
    prefilter_targets = input(DocumentSearchTarget, streaming=True)
    score_policy = input(ScorePolicy)
    scores = output(DocumentScore)

    @step(
        input=[document_scores, streamed_document_scores, online_document_scores, requests, prefilter_targets, score_policy],
        output=scores,
    )
    def merge_scores(
        self,
        stored: DocumentScore,
        streamed: DocumentScore,
        online: DocumentScore,
        request: SearchRequest,
        target: DocumentSearchTarget,
        policy: ScorePolicy,
    ) -> DocumentScore:
        candidate: DocumentScore = union_all(online)
        candidate = union_all(streamed)
        candidate = union_all(self.document_scores)
        inner_join(request, on=request.query_id == candidate.query_id)
        inner_join(
            target,
            on=(target.query_id == candidate.query_id)
            & (target.document_id == candidate.document_id)
            & (target.scope_id == candidate.scope_id),
        )
        param_join(policy)
        age = datediff(request.requested_at, candidate.scored_at)
        where(
            (candidate.scored_at <= request.requested_at)
            & (candidate.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
            & candidate.experiment_id.null_safe_eq(request.experiment_id)
        )
        drop_duplicates(candidate.query_id, candidate.document_id, candidate.experiment_id)
        return DocumentScore.project(candidate)
