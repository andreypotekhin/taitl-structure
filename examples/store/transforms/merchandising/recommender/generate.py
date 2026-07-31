from examples.store.schemas.merchandising import (
    CatalogProduct,
    ExpandedProductTaxonomy,
    ProductRecommendationSignal,
    RecommendationCandidate,
    RecommendationRequest,
    SessionFeature,
)
from structure import Transform, input, output
from structure.plugin.pyspark import coalesce, inner_join, left_join, when, where


class GenerateRecommendationCandidates(Transform):
    """Retrieve candidates from catalog, taxonomy, session interest, and feedback."""

    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    taxonomy = input(ExpandedProductTaxonomy)
    session_features = input(SessionFeature)
    signals = input(ProductRecommendationSignal)
    candidates = output(RecommendationCandidate)

    def retrieve(
        self,
        request: RecommendationRequest,
        product: CatalogProduct,
        taxonomy: ExpandedProductTaxonomy,
        session: SessionFeature,
        signal: ProductRecommendationSignal,
    ) -> RecommendationCandidate:
        inner_join(
            product,
            on=(product.tenant.tenant_id == request.tenant.tenant_id) & product.eligible,
        )
        left_join(
            taxonomy,
            on=(taxonomy.tenant.tenant_id == request.tenant.tenant_id)
            & (taxonomy.product_id == product.product_id)
            & request.category.null_safe_eq(taxonomy.ancestor_category),
        )
        left_join(
            session,
            on=(session.tenant.tenant_id == request.tenant.tenant_id)
            & session.customer_id.null_safe_eq(request.customer_id)
            & session.category.null_safe_eq(product.category),
        )
        left_join(
            signal,
            on=(signal.tenant.tenant_id == request.tenant.tenant_id)
            & (signal.strategy_id == request.strategy_id)
            & (signal.product_id == product.product_id),
        )
        where(
            product.product_id.is_not_null()
            & (request.category.is_null() | request.category.null_safe_eq(product.category))
        )
        session_match = session.session_id.is_not_null()
        category_match = request.category.null_safe_eq(product.category)
        return RecommendationCandidate.project(request, product)(
            tenant=request.tenant,
            request_id=request.id,
            requested_at=request.requested_at,
            customer_id=request.customer_id,
            session_id=request.session_id,
            strategy_id=request.strategy_id,
            policy_version=request.policy_version,
            experiment_id=request.experiment_id,
            experiment_version=request.experiment_version,
            variant_id=request.variant_id,
            category_filter=request.category,
            collection_id=request.collection_id,
            product_id=product.product_id,
            product_name=product.product_name,
            category=product.category,
            has_promotion=product.has_promotion,
            promotion_code=product.promotion_code,
            base_score=product.base_score,
            promotion_score=product.promotion_score,
            inventory_boost=0.0,
            candidate_source=when(category_match, "category").otherwise(
                when(session_match, "session").otherwise("popular")
            ),
            taxonomy_id=taxonomy.taxonomy_id,
            taxonomy_branch=coalesce(taxonomy.ancestor_category, product.category),
            session_match=session_match,
            purchase_signal=coalesce(signal.conversion_rate, 0.0),
            eligibility_status="retrieved",
        )
