from examples.store.schemas.merchandising import (
    DiversificationDecision,
    DiversifiedRecommendationCandidate,
    MerchandisingPolicy,
    RankedRecommendationCandidate,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import coalesce, inner_join, left_join, row_number, when, where


class DiversifyRecommendations(Transform):
    """Apply taxonomy-branch cap."""

    ranked = input(RankedRecommendationCandidate)
    policy = input(MerchandisingPolicy)
    decisions = lane(DiversificationDecision)
    diversified = output(DiversifiedRecommendationCandidate)
    decision_rows = output(DiversificationDecision)
    result = output(decision_rows=decisions)

    @step(output=decisions)
    def decide(self, candidate: RankedRecommendationCandidate, policy: MerchandisingPolicy) -> DiversificationDecision:
        inner_join(
            policy,
            on=(policy.tenant.tenant_id == candidate.tenant.tenant_id)
            & (policy.strategy_id == candidate.strategy_id)
            & (policy.policy_version == candidate.policy_version),
        )
        branch = coalesce(candidate.taxonomy_branch, candidate.category)
        branch_rank = row_number(
            partition_by=(candidate.tenant.tenant_id, candidate.request_id, branch),
            order_by=candidate.rank.asc(),
        )
        cap = coalesce(policy.maximum_per_taxonomy_branch, policy.maximum_results)
        selected = branch_rank <= cap
        return DiversificationDecision.project(candidate)(
            taxonomy_branch=branch,
            branch_rank=branch_rank,
            selected=selected,
            exclusion_reason=when(selected, None).otherwise("taxonomy_branch_cap"),
        )

    @step(input=[ranked, decisions], output=diversified)
    def publish(
        self, candidate: RankedRecommendationCandidate, decision: DiversificationDecision
    ) -> DiversifiedRecommendationCandidate:
        inner_join(
            decision,
            on=(decision.tenant.tenant_id == candidate.tenant.tenant_id)
            & (decision.request_id == candidate.request_id)
            & (decision.product_id == candidate.product_id),
        )
        where(decision.selected)
        return DiversifiedRecommendationCandidate.project(candidate)(
            rank=row_number(
                partition_by=(candidate.tenant.tenant_id, candidate.request_id),
                order_by=candidate.rank.asc(),
            ),
            diversity_rank=decision.branch_rank,
            diversity_selected=True,
            diversity_exclusion_reason=None,
        )
