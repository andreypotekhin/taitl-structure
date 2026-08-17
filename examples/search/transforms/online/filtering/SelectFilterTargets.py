"""Merge usable filter artifacts and expose bounded document targets."""

from typing import cast

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy
from examples.search.transforms.lib.TargetScope import target_scope_id
from structure import Transform, input, lane, output, parameter, step
from structure.plugin.pyspark import (
    coalesce,
    datediff,
    drop_duplicates,
    inner_join,
    left_join,
    param_join,
    union_all,
    where,
)


class SelectFilterTargets(Transform):
    """Merge current filter artifacts and expose bounded document targets."""

    maximum_candidates = parameter(10000)

    document_filter_scores = input(DocumentFilterScore)
    online_document_filter_scores = input(DocumentFilterScore)
    requests = input(SearchRequest, streaming=True)
    document_filter_targets = input(DocumentSearchTarget, streaming=True, optional=True)
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

    @step(input=[merged_filter_scores, document_filter_targets], output=targets)
    def select_targets(self, document: DocumentFilterScore, target: DocumentSearchTarget) -> DocumentSearchTarget:
        left_join(target, on=target.query_id == document.query_id)
        where(target.query_id.is_null() | (target.document_id == document.document_id))
        where(document.filter_rank <= self.maximum_candidates)
        return DocumentSearchTarget.project(document)(
            scope_id=coalesce(
                target.scope_id,
                target_scope_id(document.query_id, document.scored_at, cast(int, self.maximum_candidates)),
            )
        )
