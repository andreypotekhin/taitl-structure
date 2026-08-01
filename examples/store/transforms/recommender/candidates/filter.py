from examples.store.schemas.merchandising import (
    MerchandisingSuppression,
    RecommendationCandidate,
    RecommendationCandidateDecision,
    SessionFeature,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import coalesce, left_join, when, where


class FilterRecommendationCandidates(Transform):
    """Apply suppression and session exclusions."""

    candidates = input(RecommendationCandidate)
    suppressions = input(MerchandisingSuppression)
    session_features = input(SessionFeature)
    evaluated = lane(RecommendationCandidateDecision)
    decisions = output(RecommendationCandidateDecision)
    filtered = output(RecommendationCandidate)
    result = output(decisions=evaluated)

    @step(output=evaluated)
    def evaluate(
        self, candidate: RecommendationCandidate, suppression: MerchandisingSuppression, session: SessionFeature
    ) -> RecommendationCandidateDecision:
        left_join(
            suppression,
            on=(suppression.tenant.tenant_id == candidate.tenant.tenant_id)
            & (suppression.policy_version == candidate.policy_version)
            & suppression.active
            & (
                suppression.product_id.null_safe_eq(candidate.product_id)
                | suppression.category.null_safe_eq(candidate.category)
            ),
        )
        left_join(
            session,
            on=(session.tenant.tenant_id == candidate.tenant.tenant_id)
            & session.customer_id.null_safe_eq(candidate.customer_id)
            & session.product_id.null_safe_eq(candidate.product_id)
            & (session.add_to_cart_count > 0),
        )
        hard_suppression = coalesce(suppression.exclude, False)
        session_exclusion = session.product_id.is_not_null()
        eligible = ~hard_suppression & ~session_exclusion
        return RecommendationCandidateDecision.project(candidate)(
            stage="filter",
            eligible=eligible,
            exclusion_reason=when(hard_suppression, coalesce(suppression.reason, "hard_suppression")).otherwise(
                when(session_exclusion, "session_already_added").otherwise(None)
            ),
        )

    @step(input=[candidates, evaluated], output=filtered)
    def publish(
        self, candidate: RecommendationCandidate, decision: RecommendationCandidateDecision
    ) -> RecommendationCandidate:
        left_join(
            decision,
            on=(decision.tenant.tenant_id == candidate.tenant.tenant_id)
            & (decision.request_id == candidate.request_id)
            & (decision.product_id == candidate.product_id),
        )
        where(decision.eligible)
        return RecommendationCandidate.project(candidate)(eligibility_status="eligible")
