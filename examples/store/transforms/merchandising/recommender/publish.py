from examples.store.schemas.merchandising import RankedRecommendationCandidate, RecommendedProduct
from structure import *
from structure.plugin.pyspark import *


class SelectRecommendedProducts(Transform):
    ranked_candidates = input(RankedRecommendationCandidate)
    products = output(RecommendedProduct)

    def select_products(self, candidate: RankedRecommendationCandidate) -> RecommendedProduct:
        where(candidate.rank <= candidate.maximum_results)
        return RecommendedProduct.project(candidate)
