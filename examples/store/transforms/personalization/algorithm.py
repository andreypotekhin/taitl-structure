from structure import special
from structure.plugin.pyspark import abs, array_contains, coalesce, when, xxhash64


@special(type="expr")
class PersonalizationAlgorithm:
    """Replaceable expression strategy for the complete personal score."""

    algorithm_id = "hashed-latent"
    algorithm_version = "v1"

    def feature_score(self, product, preference):
        return when(
            coalesce(array_contains(preference.included_categories, product.category), False), 1.0
        ).otherwise(0.0)

    def factorization_score(self, request, product):
        identity = coalesce(request.customer_id, request.session_id)
        bucket = abs(xxhash64(request.tenant.tenant_id, identity, product.product_id)) % 100
        return when(identity.is_not_null(), bucket / 100.0).otherwise(0.0)

    def personal_score(self, feature_score, history_score, factorization_score, excluded):
        return when(excluded, 0.0).otherwise(
            feature_score + history_score * 0.1 + factorization_score * 0.1
        )
