from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class TaxonomyNode(Schema):
    tenant = struct(TenantKey, nullable=False)
    taxonomy_id = string(nullable=False)
    parent_taxonomy_id = string(nullable=True)
    category = string(nullable=False)
    active = boolean(nullable=False)


class ProductTaxonomy(Schema):
    tenant = struct(TenantKey, nullable=False)
    product_id = string(nullable=False)
    taxonomy_id = string(nullable=False)
    category = string(nullable=False)


class TaxonomyAncestor(Schema):
    node_id = string(nullable=False)
    ancestor_id = string(nullable=False)
    depth = long(nullable=False)


class ExpandedProductTaxonomy(Schema):
    tenant = struct(TenantKey, nullable=False)
    product_id = string(nullable=False)
    taxonomy_id = string(nullable=False)
    normalized_category = string(nullable=False)
    ancestor_taxonomy_id = string(nullable=False)
    ancestor_category = string(nullable=False)
    ancestor_depth = long(nullable=False)
