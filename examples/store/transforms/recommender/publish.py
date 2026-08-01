from examples.store.schemas.merchandising import DiversifiedRecommendationCandidate, RecommendedProduct
from structure import *
from structure.plugin.pyspark import *


class SelectRecommendedProducts(Transform):
    ranked_candidates = input(DiversifiedRecommendationCandidate)
    products = output(RecommendedProduct)

    def select_products(self, candidate: DiversifiedRecommendationCandidate) -> RecommendedProduct:
        where(candidate.rank <= candidate.maximum_results)
        return RecommendedProduct.project(candidate)
