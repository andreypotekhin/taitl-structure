from examples.store.schemas.catalog import CatalogProduct
from examples.store.schemas.merchandising import RecommendationCandidate, RecommendationRequest
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
            candidate_source="catalog",
            taxonomy_id=None,
            taxonomy_branch=product.category,
            session_match=False,
            purchase_signal=0.0,
            eligibility_status="eligible",
        )
