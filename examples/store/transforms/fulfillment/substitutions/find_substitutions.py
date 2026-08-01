from examples.store.schemas.fulfillment.demand import Order
from examples.store.schemas.fulfillment.inventory import InventoryPosition
from examples.store.schemas.fulfillment.substitutions import FulfillmentSubstitutionOption, SubstitutionRule
from structure import *
from structure.plugin.pyspark import *


class FindSubstitutions(Transform):
    """Find policy-approved alternatives without changing the original demand line."""

    demand = input(Order)
    rules = input(SubstitutionRule)
    inventory_positions = input(InventoryPosition)
    candidates = lane(FulfillmentSubstitutionOption)
    options = output(FulfillmentSubstitutionOption)

    @step(input=[demand, rules, inventory_positions], output=candidates)
    def find_candidates(
        self, order: Order, rule: SubstitutionRule, inventory: InventoryPosition
    ) -> FulfillmentSubstitutionOption:
        inner_join(
            rule,
            on=(rule.tenant.tenant_id == order.tenant.tenant_id)
            & (rule.product_id == order.product_id)
            & rule.active,
        )
        left_join(
            inventory,
            on=(inventory.tenant.tenant_id == order.tenant.tenant_id)
            & (inventory.product_id == rule.substitute_product_id),
        )
        group_by(
            tenant_id=order.tenant.tenant_id,
            order_id=order.order_id,
            line_number=order.line_number,
            customer_id=order.customer_id,
            original_product_id=order.product_id,
            substitute_product_id=rule.substitute_product_id,
            equivalence_group=rule.equivalence_group,
            policy_rank=rule.policy_rank,
        )
        available = coalesce(
            when(
                inventory.on_hand_quantity > inventory.reserved_quantity,
                inventory.on_hand_quantity - inventory.reserved_quantity,
            ).otherwise(0),
            0,
        )
        return FulfillmentSubstitutionOption.project(order)(
            original_product_id=order.product_id,
            substitute_product_id=rule.substitute_product_id,
            equivalence_group=rule.equivalence_group,
            policy_rank=rule.policy_rank,
            available_to_promise=sum(available),
            option_rank=sum(0),
            reason=min("policy_approved_substitute"),
        )

    @step(input=candidates, output=options)
    def rank_candidates(self, option: FulfillmentSubstitutionOption) -> FulfillmentSubstitutionOption:
        return FulfillmentSubstitutionOption.project(option)(
            option_rank=row_number(
                partition_by=(option.tenant.tenant_id, option.order_id, option.line_number),
                order_by=(
                    option.policy_rank.asc(),
                    option.substitute_product_id.asc(),
                    option.available_to_promise.desc(),
                ),
            )
        )
