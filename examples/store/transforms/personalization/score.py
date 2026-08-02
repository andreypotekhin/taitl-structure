from examples.store.schemas.catalog import *
from examples.store.schemas.merchandising import *
from examples.store.schemas.personalization import *
from structure import *
from structure.plugin.pyspark import *


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


class ScorePersonalizedRecommendations(Transform):
    """Score eligible catalog products for one recommendation request."""

    algorithm = parameter(PersonalizationAlgorithm())

    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    preferences = input(UserFeaturePreference)
    history = input(PersonalizationHistory, streaming=True)
    recommendations = output(PersonalizedRecommendation)

    def score(
        self,
        request: RecommendationRequest,
        product: CatalogProduct,
        preference: UserFeaturePreference,
        interaction: PersonalizationHistory,
    ) -> PersonalizedRecommendation:
        inner_join(
            product,
            on=(product.tenant.tenant_id == request.tenant.tenant_id) & product.eligible,
        )
        left_join(
            preference,
            on=(preference.tenant.tenant_id == request.tenant.tenant_id)
            & request.customer_id.is_not_null()
            & (preference.customer_id == request.customer_id),
        )
        left_join(
            interaction,
            on=(interaction.tenant.tenant_id == request.tenant.tenant_id)
            & interaction.category.null_safe_eq(product.category)
            & (
                (
                    request.customer_id.is_not_null()
                    & interaction.customer_id.null_safe_eq(request.customer_id)
                )
                | (
                    request.session_id.is_not_null()
                    & interaction.session_id.null_safe_eq(request.session_id)
                )
            ),
        )
        matched = coalesce(array_contains(preference.included_categories, product.category), False)
        excluded = coalesce(array_contains(preference.excluded_categories, product.category), False)
        feature_score = self.algorithm.feature_score(product, preference)
        history_score = coalesce(interaction.history_score, 0.0)
        factorization_score = coalesce(self.algorithm.factorization_score(request, product), 0.0)
        return PersonalizedRecommendation(
            tenant=request.tenant,
            request_id=request.id,
            customer_id=request.customer_id,
            session_id=request.session_id,
            product_id=product.product_id,
            feature_score=feature_score,
            history_score=history_score,
            factorization_score=factorization_score,
            personal_score=self.algorithm.personal_score(
                feature_score,
                history_score,
                factorization_score,
                excluded,
            ),
            matched_category=matched,
            excluded_by_preference=excluded,
            algorithm_id=self.algorithm.algorithm_id,
            algorithm_version=self.algorithm.algorithm_version,
        )
