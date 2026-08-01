from examples.store.schemas.merchandising import (
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RankedRecommendationCandidate,
    RecommendationCandidate,
)
from examples.store.transforms.recommender.ranking.ranker import Ranker
from structure import *
from structure.plugin.pyspark import *


class RankRecommendationCandidates(Transform):
    ranker = parameter(Ranker())

    candidates = input(RecommendationCandidate)
    policy = input(MerchandisingPolicy)
    boosts = input(MerchandisingBoost)
    suppressions = input(MerchandisingSuppression)
    signals = input(ProductRecommendationSignal)
    ranked_candidates = output(RankedRecommendationCandidate)

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
            & (boost.product_id.null_safe_eq(candidate.product_id) | boost.category.null_safe_eq(candidate.category)),
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
        boost_score = self.ranker.boost_score(boost)
        suppression_penalty = self.ranker.suppression_penalty(suppression)
        supported = self.ranker.feedback_supported(signal, policy)
        feedback_score = self.ranker.feedback_score(signal, policy, supported)
        final_score = self.ranker.final_score(candidate, boost_score, suppression_penalty, feedback_score)
        return RankedRecommendationCandidate.project(candidate)(
            rank=row_number(
                partition_by=(candidate.tenant.tenant_id, candidate.request_id),
                order_by=self.ranker.order_by(candidate, final_score, suppression_penalty),
            ),
            boost_score=boost_score,
            suppression_penalty=suppression_penalty,
            feedback_score=feedback_score,
            final_score=final_score,
            feedback_contributed=supported,
            maximum_results=policy.maximum_results,
            diversity_selected=True,
            diversity_exclusion_reason=None,
        )
