from examples.store.schemas.merchandising import (
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RankedRecommendationCandidate,
    RecommendationCandidate,
)
from structure import *
from structure.plugin.pyspark import *


@special(type="expr")
class Ranker:
    def boost_score(self, boost):
        return coalesce(boost.boost_score, 0.0)

    def suppression_penalty(self, suppression):
        return coalesce(suppression.penalty, 0.0)

    def feedback_supported(self, signal, policy):
        return coalesce(signal.impression_count, 0) >= policy.minimum_feedback_impressions

    def feedback_score(self, signal, policy, supported):
        return coalesce(
            when(
                supported & signal.click_through_rate.is_not_null(),
                signal.click_through_rate * policy.feedback_weight,
            ).otherwise(0.0),
            0.0,
        )

    def final_score(self, candidate, boost_score, suppression_penalty, feedback_score):
        return (
            candidate.base_score
            + candidate.promotion_score
            + boost_score
            + candidate.inventory_boost
            + feedback_score
            - suppression_penalty
        )

    def order_by(self, candidate, final_score, suppression_penalty):
        return (
            final_score.desc(),
            suppression_penalty.asc(),
            candidate.inventory_boost.desc(),
            candidate.product_id.asc(),
        )


class RankRecommendationCandidates(Transform):
    ranker = Ranker()

    candidates = input(RecommendationCandidate)
    policy = input(MerchandisingPolicy)
    boosts = input(MerchandisingBoost)
    suppressions = input(MerchandisingSuppression)
    signals = input(ProductRecommendationSignal)
    ranked_candidates = output(RankedRecommendationCandidate)

    def __init__(self, *, ranker: Ranker | None = None, **inputs: object) -> None:
        super().__init__(**inputs)
        self.ranker = ranker or type(self).ranker

    def rank(
        self,
        candidate: RecommendationCandidate,
        policy: MerchandisingPolicy,
        boost: MerchandisingBoost,
        suppression: MerchandisingSuppression,
        signal: ProductRecommendationSignal,
    ) -> RankedRecommendationCandidate:
        inner_join(
            policy,
            on=(policy.tenant.tenant_id == candidate.tenant.tenant_id)
            & (policy.strategy_id == candidate.strategy_id)
            & (policy.policy_version == candidate.policy_version),
        )
        left_join(
            boost,
            on=(boost.tenant.tenant_id == candidate.tenant.tenant_id)
            & (boost.policy_version == policy.policy_version)
            & boost.active
            & (
                boost.product_id.null_safe_eq(candidate.product_id)
                | boost.category.null_safe_eq(candidate.category)
            ),
        )
        left_join(
            suppression,
            on=(suppression.tenant.tenant_id == candidate.tenant.tenant_id)
            & (suppression.policy_version == policy.policy_version)
            & suppression.active
            & (
                suppression.product_id.null_safe_eq(candidate.product_id)
                | suppression.category.null_safe_eq(candidate.category)
            ),
        )
        where(coalesce(suppression.exclude, False) == False)
        left_join(
            signal,
            on=(signal.tenant.tenant_id == candidate.tenant.tenant_id)
            & (signal.strategy_id == candidate.strategy_id)
            & (signal.product_id == candidate.product_id),
        )
        ranker = self.ranker
        boost_score = ranker.boost_score(boost)
        suppression_penalty = ranker.suppression_penalty(suppression)
        supported = ranker.feedback_supported(signal, policy)
        feedback_score = ranker.feedback_score(signal, policy, supported)
        final_score = ranker.final_score(candidate, boost_score, suppression_penalty, feedback_score)
        return RankedRecommendationCandidate.project(candidate)(
            policy_version=policy.policy_version,
            rank=row_number(
                partition_by=(candidate.tenant.tenant_id, candidate.request_id),
                order_by=ranker.order_by(candidate, final_score, suppression_penalty),
            ),
            boost_score=boost_score,
            suppression_penalty=suppression_penalty,
            feedback_score=feedback_score,
            final_score=final_score,
            feedback_contributed=supported,
            maximum_results=policy.maximum_results,
        )
