from examples.store.schemas.catalog import CatalogProduct
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from structure import *
from structure.plugin.pyspark import *


class PrepareCatalog(Transform):
    products = input(Product)
    blocked_products = input(BlockedProduct)
    promotions = input(Promotion)
    catalog = output(CatalogProduct)

    @special(type="expr")
    def clean(value):
        return lower(trim(value))

    def prepare(self, product: Product, blocked_product: BlockedProduct, promotion: Promotion) -> CatalogProduct:
        where(product.active)
        where(
            not_exists(
                on=(blocked_product.tenant.tenant_id == product.tenant.tenant_id) & (blocked_product.id == product.id)
            )
        )
        left_join(
            promotion,
            on=(promotion.tenant.tenant_id == product.tenant.tenant_id)
            & (
                (self.clean(promotion.code) == self.clean(product.id))
                | self.clean(promotion.code).null_safe_eq(self.clean(product.category))
            ),
        )
        has_promotion = promotion.code.is_not_null()
        return CatalogProduct.project(product)(
            product_id=product.id,
            product_name=product.name,
            features=coalesce(product.features, arr_compact(array(product.category))),
            has_promotion=has_promotion,
            promotion_code=promotion.code,
            promotion_name=promotion.name,
            promotion_discount=promotion.discount,
            base_score=1.0,
            promotion_score=when(has_promotion, 0.5).otherwise(0.0),
            eligible=True,
        )
