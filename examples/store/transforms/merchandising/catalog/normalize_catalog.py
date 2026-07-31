from examples.store.schemas.merchandising import CatalogProduct
from structure import Transform, input, output, special
from structure.plugin.pyspark import lower, trim


class NormalizeCatalog(Transform):
    """Canonicalize the identifiers used by recommendation joins."""

    catalog = input(CatalogProduct)
    normalized = output(CatalogProduct)

    @special(type="expr")
    def clean(value):
        return lower(trim(value))

    def normalize(self, product: CatalogProduct) -> CatalogProduct:
        return CatalogProduct.project(product)(
            product_id=self.clean(product.product_id),
            category=self.clean(product.category),
            promotion_code=self.clean(product.promotion_code),
        )
