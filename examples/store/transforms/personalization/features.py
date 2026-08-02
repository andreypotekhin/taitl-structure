from examples.store.schemas.catalog import *
from structure import *
from structure.plugin.pyspark import *


class BuildProductFeatures(Transform):
    """Add normalized category tokens to caller-supplied catalog features."""

    catalog = input(CatalogProduct)
    featured = output(CatalogProduct)

    def build(self, product: CatalogProduct) -> CatalogProduct:
        return CatalogProduct.project(product)(
            features=array_union(product.features, arr_compact(array(product.category))),
        )
