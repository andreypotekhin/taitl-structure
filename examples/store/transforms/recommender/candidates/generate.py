from examples.store.schemas.merchandising import ProductRecommendationSignal, RecommendationCandidate, SessionFeature
from examples.store.schemas.taxonomy import ExpandedProductTaxonomy
from structure import Transform, input, output
from structure.plugin.pyspark import coalesce, left_join, when


class GenerateRecommendationCandidates(Transform):
    """Enrich admitted candidates with taxonomy, session interest, and feedback signals."""

    admitted = input(RecommendationCandidate, streaming=True)
    taxonomy = input(ExpandedProductTaxonomy)
    session_features = input(SessionFeature)
    signals = input(ProductRecommendationSignal)
    candidates = output(RecommendationCandidate)

    def retrieve(
        self,
        candidate: RecommendationCandidate,
        taxonomy: ExpandedProductTaxonomy,
        session: SessionFeature,
        signal: ProductRecommendationSignal,
    ) -> RecommendationCandidate:
        left_join(
            taxonomy,
            on=(taxonomy.tenant.tenant_id == candidate.tenant.tenant_id)
            & (taxonomy.product_id == candidate.product_id)
            & candidate.category_filter.null_safe_eq(taxonomy.ancestor_category),
        )
        left_join(
            session,
            on=(session.tenant.tenant_id == candidate.tenant.tenant_id)
            & session.customer_id.null_safe_eq(candidate.customer_id)
            & session.category.null_safe_eq(candidate.category),
        )
        left_join(
            signal,
            on=(signal.tenant.tenant_id == candidate.tenant.tenant_id)
            & (signal.strategy_id == candidate.strategy_id)
            & (signal.product_id == candidate.product_id),
        )
        session_match = session.session_id.is_not_null()
        category_match = candidate.category_filter.null_safe_eq(candidate.category)
        return RecommendationCandidate.project(candidate)(
            inventory_boost=0.0,
            candidate_source=when(category_match, "category").otherwise(
                when(session_match, "session").otherwise("popular")
            ),
            taxonomy_id=taxonomy.taxonomy_id,
            taxonomy_branch=coalesce(taxonomy.ancestor_category, candidate.category),
            session_match=session_match,
            purchase_signal=coalesce(signal.conversion_rate, 0.0),
            eligibility_status="retrieved",
        )
