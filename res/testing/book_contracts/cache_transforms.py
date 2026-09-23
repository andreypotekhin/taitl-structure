from structure import *
from structure.plugin.pyspark import *
from testing.book_contracts.transforms import Order, UniqueOrders


class CacheOrders(UniqueOrders):
    def check(self, order: Order) -> Order:
        cache()
        return order


class ReleaseOrders(UniqueOrders):
    def check(self, order: Order) -> Order:
        cache()
        unpersist(blocking=True)
        return order

