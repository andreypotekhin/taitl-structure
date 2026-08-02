from examples.store.schemas.catalog import CatalogProduct
from structure import Transform, input, output
from structure.plugin.pyspark import arr_compact, array, array_union


class BuildProductFeatures(Transform):
    """Add normalized category tokens to caller-supplied catalog features."""

    catalog = input(CatalogProduct)
    featured = output(CatalogProduct)

    def build(self, product: CatalogProduct) -> CatalogProduct:
        return CatalogProduct.project(product)(
            features=array_union(product.features, arr_compact(array(product.category))),
        )
