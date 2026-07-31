from examples.store.schemas.merchandising import CatalogProduct, RecommendationCandidate, RecommendationRequest
from structure import *
from structure.plugin.pyspark import *


class SelectRecommendationCandidates(Transform):
    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    candidates = output(RecommendationCandidate)

    def select(self, request: RecommendationRequest, product: CatalogProduct) -> RecommendationCandidate:
        inner_join(
            product,
            on=(product.tenant.tenant_id == request.tenant.tenant_id)
            & product.eligible
            & (request.category.is_null() | request.category.null_safe_eq(product.category)),
        )
        return RecommendationCandidate.project(request, product)(
            tenant=request.tenant,
            request_id=request.id,
            category_filter=request.category,
            category=product.category,
            inventory_boost=0.0,
        )
