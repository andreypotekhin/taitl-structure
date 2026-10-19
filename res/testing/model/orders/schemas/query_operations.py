from structure import Schema
from structure.plugin.pyspark import *


class Store(Schema):
    store_id = string(nullable=False)


class Inventory(Schema):
    inventory_id = long(nullable=False)
    store_id = string(nullable=False)
    status = string(nullable=False)
    item_name = string(nullable=False)
    quantity = long(nullable=False)


class ClearanceItem(Schema):
    item_name = string(nullable=False)
    quantity = long(nullable=False)


class StoreItem(Schema):
    store_id = string(nullable=False)
    item_name = string(nullable=True)
