from structure import special
from structure.plugin.pyspark import coalesce, when


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
