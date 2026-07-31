from examples.store.schemas.merchandising import (
    MerchandisingPolicy,
    RecommendationRequest,
    RecommendationRun,
    RecommendedProduct,
)
from structure import *
from structure.plugin.pyspark import *


class SummarizeRecommendationRuns(Transform):
    requests = input(RecommendationRequest, streaming=True)
    policy = input(MerchandisingPolicy)
    products = input(RecommendedProduct)
    runs = output(RecommendationRun)

    def summarize(
        self, request: RecommendationRequest, policy: MerchandisingPolicy, product: RecommendedProduct
    ) -> RecommendationRun:
        inner_join(
            policy,
            on=(policy.tenant.tenant_id == request.tenant.tenant_id)
            & (policy.strategy_id == request.strategy_id)
            & (policy.policy_version == request.policy_version),
        )
        left_join(
            product,
            on=(product.tenant.tenant_id == request.tenant.tenant_id)
            & (product.request_id == request.id)
            & (product.policy_version == policy.policy_version),
        )
        group_by(
            tenant_id=request.tenant.tenant_id,
            request_id=request.id,
            strategy_id=request.strategy_id,
            policy_version=policy.policy_version,
            experiment_id=request.experiment_id,
            experiment_version=request.experiment_version,
            variant_id=request.variant_id,
        )
        return RecommendationRun(
            tenant=request.tenant,
            request_id=request.id,
            strategy_id=request.strategy_id,
            policy_version=policy.policy_version,
            result_count=sum(when(product.product_id.is_not_null(), 1).otherwise(0)),
            feedback_contributed=bool_or(coalesce(product.feedback_contributed, False)),
            experiment_id=request.experiment_id,
            experiment_version=request.experiment_version,
            variant_id=request.variant_id,
        )
