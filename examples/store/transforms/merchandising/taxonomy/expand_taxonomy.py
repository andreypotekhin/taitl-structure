from examples.store.schemas.merchandising.taxonomy import (
    ExpandedProductTaxonomy,
    ProductTaxonomy,
    TaxonomyAncestor,
    TaxonomyNode,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import hierarchy_closure, inner_join, where


class ExpandProductTaxonomy(Transform):
    """Expand each product category to its bounded taxonomy ancestors."""

    product_taxonomy = input(ProductTaxonomy)
    taxonomy = input(TaxonomyNode)
    ancestors = lane(TaxonomyAncestor)
    expanded = output(ExpandedProductTaxonomy)

    @step(output=ancestors)
    def build_ancestors(self, node: TaxonomyNode) -> TaxonomyAncestor:
        where(node.active)
        closure = hierarchy_closure(
            node.taxonomy_id,
            parent=node.parent_taxonomy_id,
            as_=TaxonomyAncestor,
            node="node_id",
            ancestor="ancestor_id",
            max_depth=16,
            scope="taxonomy_ancestors",
        )
        return TaxonomyAncestor.project(closure)

    @step(input=[product_taxonomy, ancestors, taxonomy], output=expanded)
    def expand(
        self, product: ProductTaxonomy, ancestor: TaxonomyAncestor, node: TaxonomyNode
    ) -> ExpandedProductTaxonomy:
        inner_join(ancestor, on=ancestor.node_id == product.taxonomy_id)
        inner_join(
            node,
            on=(node.tenant.tenant_id == product.tenant.tenant_id)
            & (node.taxonomy_id == ancestor.ancestor_id)
            & node.active,
        )
        return ExpandedProductTaxonomy(
            tenant=product.tenant,
            product_id=product.product_id,
            taxonomy_id=product.taxonomy_id,
            normalized_category=product.category,
            ancestor_taxonomy_id=ancestor.ancestor_id,
            ancestor_category=node.category,
            ancestor_depth=ancestor.depth,
        )
