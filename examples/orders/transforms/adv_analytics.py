import structure
from examples.orders.schemas.adv_analytics import (
    OrderCollectionProfile,
    OrderCollectionSource,
    OrderCustomerWindow,
    OrderProductCube,
    OrderRevenueRollup,
)
from examples.orders.schemas.order import OrderFulfillment


class AdvancedOrderAnalytics(structure.Transform):
    fulfilled = structure.input(OrderFulfillment)
    collections = structure.input(OrderCollectionSource)
    revenue_rollups = structure.output(OrderRevenueRollup)
    product_cubes = structure.output(OrderProductCube)
    customer_windows = structure.output(OrderCustomerWindow)
    collection_profiles = structure.output(OrderCollectionProfile)

    @structure.step(input=fulfilled, output=revenue_rollups)
    def revenue_rollup(self, order: OrderFulfillment) -> OrderRevenueRollup:
        structure.rollup(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            order_date=order.business.order_date,
        )

        return OrderRevenueRollup(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            order_date=order.business.order_date,
            grouping_id=structure.grouping_id(),
            category_subtotal=structure.is_grouped(order.product_category),
            order_count=structure.count(),
            large_order_count=structure.count(where=order.is_large),
            large_units=structure.sum(order.quantity, where=order.is_large),
            any_large_order=structure.bool_or(order.is_large),
            all_large_orders=structure.bool_and(order.is_large),
            quantity_stddev=structure.stddev(order.quantity),
            quantity_variance=structure.variance(order.quantity),
            quantity_median=structure.approx_percentile(order.quantity, 0.5, accuracy=100),
            quantity_total=structure.sum(order.quantity),
            quantity_price_corr=structure.corr(order.quantity, order.product_list_price),
            quantity_price_covar=structure.covar(order.quantity, order.product_list_price),
            estimated_customers=structure.approx_count_distinct(order.customer_id),
            first_customer_id=structure.first_value(order.customer_id, order_by=order.quantity),
            last_customer_id=structure.last_value(order.customer_id, order_by=order.quantity),
            customer_ids=structure.collect_set(order.customer_id),
            order_ids=structure.collect_list(order.id),
        )

    @structure.step(input=fulfilled, output=product_cubes)
    def product_cube(self, order: OrderFulfillment) -> OrderProductCube:
        structure.cube(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            customer_tier=order.customer_tier,
        )

        return OrderProductCube(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            customer_tier=order.customer_tier,
            grouping_id=structure.grouping_id(),
            order_count=structure.count(),
            distinct_customers=structure.count_distinct(order.customer_id),
            gross_total=structure.sum(order.total),
        )

    @structure.step(input=fulfilled, output=customer_windows)
    def customer_window(self, order: OrderFulfillment) -> OrderCustomerWindow:
        customer_window = structure.window(
            partition_by=order.customer_id,
            order_by=order.quantity,
            frame=structure.rows_between(structure.preceding(2), structure.current_row()),
        )

        return OrderCustomerWindow(
            tenant_id=order.tenant.tenant_id,
            customer_id=order.customer_id,
            order_id=order.id,
            quantity=order.quantity,
            percent_rank=structure.percent_rank(over=customer_window),
            cume_dist=structure.cume_dist(over=customer_window),
            quantity_tile=structure.ntile(2, over=customer_window),
            first_order_id=structure.first_value(order.id, over=customer_window),
            last_order_id=structure.last_value(order.id, over=customer_window),
            second_order_id=structure.nth_value(order.id, 2, over=customer_window),
            running_units=structure.window_sum(order.quantity, over=customer_window),
            running_avg_units=structure.window_avg(order.quantity, over=customer_window),
            running_min_units=structure.window_min(order.quantity, over=customer_window),
            running_max_units=structure.window_max(order.quantity, over=customer_window),
            running_order_count=structure.window_count(over=customer_window),
        )

    @structure.step(input=collections, output=collection_profiles)
    def collection_profile(self, row: OrderCollectionSource) -> OrderCollectionProfile:
        normalized_attributes = structure.map_filter(
            structure.map_transform_keys(
                structure.map_transform_values(
                    row.attributes, lambda key, value: structure.lower(structure.trim(value))
                ),
                lambda key, value: structure.lower(structure.trim(key)),
            ),
            lambda key, value: value.is_not_null(),
        )

        return OrderCollectionProfile(
            id=row.id,
            normalized_tags=structure.arr_distinct(
                structure.arr_zip_with(row.tags, row.tags, lambda left, right: structure.lower(structure.trim(left)))
            ),
            sorted_tags=structure.arr_sort_by(row.tags, lambda tag: structure.lower(structure.trim(tag))),
            flat_tags=structure.arr_flatten(row.nested_tags),
            score_total=structure.arr_aggregate(row.scores, 0, lambda acc, item: acc + item),
            tag_position=structure.arr_position(row.tags, "priority"),
            has_priority=structure.arr_exists(row.tags, lambda tag: structure.lower(structure.trim(tag)) == "priority"),
            all_tags_present=structure.arr_forall(row.tags, lambda tag: tag.is_not_null()),
            normalized_attributes=normalized_attributes,
            zipped_attributes=structure.map_zip_with(
                row.attributes, row.attributes, lambda key, left, right: structure.lower(structure.trim(left))
            ),
            attribute_keys=structure.map_keys(row.attributes),
            attribute_values=structure.map_values(row.attributes),
            roundtrip_attributes=structure.map_from_entries(structure.map_entries(row.attributes)),
        )
