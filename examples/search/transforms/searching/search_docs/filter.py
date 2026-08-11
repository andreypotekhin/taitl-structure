"""Cached and online simple-overlap document filtering."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import datediff, drop_duplicates, inner_join, param_join, union_all, where


class SelectFilterTargets(Transform):
    """Merge usable filter artifacts and expose bounded document targets."""

    maximum_candidates = 10000

    document_filter_scores = input(DocumentFilterScore)
    online_document_filter_scores = input(DocumentFilterScore)
    requests = input(SearchRequest, streaming=True)
    score_policy = input(ScorePolicy)
    merged_filter_scores = lane(DocumentFilterScore)
    targets = output(DocumentSearchTarget)

    @step(
        input=[document_filter_scores, online_document_filter_scores, requests, score_policy],
        output=merged_filter_scores,
    )
    def merge_filter_scores(
        self,
        stored: DocumentFilterScore,
        online: DocumentFilterScore,
        request: SearchRequest,
        policy: ScorePolicy,
    ) -> DocumentFilterScore:
        candidate: DocumentFilterScore = union_all(online)
        inner_join(request, on=request.query_id == candidate.query_id)
        param_join(policy)
        age = datediff(request.requested_at, candidate.scored_at)
        where(
            (candidate.scored_at <= request.requested_at)
            & (candidate.scored_at >= policy.effective_at)
            & (age >= 0)
            & (age <= policy.maximum_age_days)
        )
        drop_duplicates(candidate.query_id, candidate.document_id)
        return DocumentFilterScore.project(candidate)

    @step(input=merged_filter_scores, output=targets)
    def select_targets(self, document: DocumentFilterScore) -> DocumentSearchTarget:
        where(document.filter_rank <= self.maximum_candidates)
        return DocumentSearchTarget.project(document)
