from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = string(nullable=False)
    revision = long(nullable=False)
    amount = long(nullable=True)


class Reference(Schema):
    id = string(nullable=False)


class UniqueOrders(Transform):
    orders = input(Order)
    checked = output(Order)

    def check(self, order: Order) -> Order:
        require_unique(order.id, order.revision)
        return order


class PositiveOrders(Transform):
    orders = input(Order)
    checked = output(Order)

    def check(self, order: Order) -> Order:
        require_all(order.amount >= 0)
        return order


class ReferencedOrders(Transform):
    orders = input(Order)
    references = input(Reference)
    checked = output(Order)

    def check(self, order: Order, reference: Reference) -> Order:
        require_reference(order.id, reference, reference_key=reference.id)
        return order


class LatestOrders(Transform):
    orders = input(Order)
    checked = output(Order)

    def choose(self, order: Order) -> Order:
        latest_by(order.revision, partition_by=order.id)
        return order


class EarliestOrders(LatestOrders):
    def choose(self, order: Order) -> Order:
        earliest_by(order.revision, partition_by=order.id)
        return order


class BrokenHook(Transform):
    orders = input(Order)
    checked = output(Order)

    @raw(inout=input(orders) | lane(orders), target="pyspark")
    def remove_amount(self, *, orders, spark, ctx):
        return orders.drop("amount")

    def publish(self, order: Order) -> Order:
        return order


class Price(Schema):
    id = string(nullable=False)
    start = long(nullable=False)
    end = long(nullable=True)
    price = long(nullable=False)


class PricedOrder(Order):
    price = long(nullable=True)


class TemporalOrders(Transform):
    orders = input(Order)
    prices = input(Price)
    checked = output(PricedOrder)

    def price(self, order: Order, price: Price) -> PricedOrder:
        temporal_one(
            price, on=order.id == price.id, at=order.revision,
            valid_from=price.start, valid_to=price.end, overlaps="error",
        )
        return PricedOrder.project(order)(price=price.price)


class AsOfOrders(TemporalOrders):
    def price(self, order: Order, price: Price) -> PricedOrder:
        as_of_one(price, on=order.id == price.id, left_time=order.revision, right_time=price.start)
        return PricedOrder.project(order)(price=price.price)


class NearestOrders(TemporalOrders):
    def price(self, order: Order, price: Price) -> PricedOrder:
        as_of_one(price, on=order.id == price.id, left_time=order.revision,
                  right_time=price.start, direction="nearest")
        return PricedOrder.project(order)(price=price.price)


class ForwardOrders(TemporalOrders):
    def price(self, order: Order, price: Price) -> PricedOrder:
        as_of_one(price, on=order.id == price.id, left_time=order.revision,
                  right_time=price.start, direction="forward")
        return PricedOrder.project(order)(price=price.price)


class LookupOrders(TemporalOrders):
    def price(self, order: Order, price: Price) -> PricedOrder:
        lookup_join(price, on=order.id == price.id, dedupe=JoinDedupe.latest_by(price.start))
        return PricedOrder.project(order)(price=price.price)


class FirstAmount(Transform):
    orders = input(Order)
    checked = output(Order)

    def summarize(self, order: Order) -> Order:
        group_by(id=order.id)
        return Order(id=order.id, revision=min(order.revision),
                     amount=first_value(order.amount, order_by=order.revision))


class ExtraHook(BrokenHook):
    @raw(inout=input(BrokenHook.orders) | lane(BrokenHook.orders), target="pyspark",
         schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
    def remove_amount(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F
        return orders.withColumn("extra", F.lit(1)).select("extra", "amount", "revision", "id")


class WrongTypeHook(BrokenHook):
    @raw(inout=input(BrokenHook.orders) | lane(BrokenHook.orders), target="pyspark")
    def remove_amount(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F
        return orders.withColumn("amount", F.col("amount").cast("string"))


class WrongReturnHook(BrokenHook):
    @raw(inout=input(BrokenHook.orders) | lane(BrokenHook.orders), target="pyspark")
    def remove_amount(self, *, orders, spark, ctx):
        return None


class StrictExtraHook(BrokenHook):
    @raw(inout=input(BrokenHook.orders) | lane(BrokenHook.orders), target="pyspark")
    def remove_amount(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F
        return orders.withColumn("extra", F.lit(1))


class Subtotal(Schema):
    amount = long(nullable=True)
    omitted = boolean(nullable=False)
    mask = long(nullable=False)
    total = long(nullable=True)


class RollupOrders(Transform):
    orders = input(Order)
    checked = output(Subtotal)

    def summarize(self, order: Order) -> Subtotal:
        rollup(amount=order.amount)
        return Subtotal(amount=order.amount, omitted=is_grouped(order.amount),
                        mask=grouping_id(), total=sum(order.revision))


class CubeOrders(RollupOrders):
    def summarize(self, order: Order) -> Subtotal:
        cube(amount=order.amount)
        return Subtotal(amount=order.amount, omitted=is_grouped(order.amount),
                        mask=grouping_id(), total=sum(order.revision))


class GroupingSetsOrders(RollupOrders):
    def summarize(self, order: Order) -> Subtotal:
        grouping_sets((order.amount,), ())
        return Subtotal(amount=order.amount, omitted=is_grouped(order.amount),
                        mask=grouping_id(), total=sum(order.revision))


class OnlyOrder(UniqueOrders):
    def check(self, order: Order) -> Order:
        exactly_one(self.orders)
        return order


class PositiveThenEmpty(PositiveOrders):
    def empty(self, order: Order) -> Order:
        where(order.revision < 0)
        return order


class PositiveThenFilter(PositiveOrders):
    def positive(self, order: Order) -> Order:
        where(order.amount >= 0)
        return order




class Node(Schema):
    id = string(nullable=False)
    parent = string(nullable=True)
    priority = long(nullable=False)


class CheckHierarchy(Transform):
    orders = input(Node)
    checked = output(Node)

    def check(self, node: Node) -> Node:
        require_parent_hierarchy(node.id, parent=node.parent, order_by=node.priority, max_depth=2)
        return node


class PriorityOrders(UniqueOrders):
    def check(self, order: Order) -> Order:
        select_first_qualified(order.id, where=order.amount >= 0, order_by=order.revision, missing="error")
        return order


class NullableOrder(Order):
    id = string(nullable=True)
    revision = long(nullable=True)


class NullableLatest(Transform):
    orders = input(NullableOrder)
    checked = output(NullableOrder)

    def choose(self, order: NullableOrder) -> NullableOrder:
        latest_by(order.revision, partition_by=order.id)
        return order


class NullableReference(Transform):
    orders = input(NullableOrder)
    references = input(Reference)
    checked = output(NullableOrder)

    def check(self, order: NullableOrder, reference: Reference) -> NullableOrder:
        require_reference(order.id, reference, reference_key=reference.id)
        return order


class RejectNullReference(NullableReference):
    def check(self, order: NullableOrder, reference: Reference) -> NullableOrder:
        require_reference(order.id, reference, reference_key=reference.id, nulls="reject")
        return order


class MultipleHook(Transform):
    orders = input(Order)
    checked = output(Order)
    audit = output(Order)

    @step(inout=orders | [checked, audit])
    def split(self, order: Order) -> tuple[Order, Order]:
        return order, order

    @raw(inout=lane(checked) | [output(checked), output(audit)], target="pyspark")
    def polish(self, *, checked, audit, spark, ctx):
        return (checked,)
