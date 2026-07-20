from examples.orders.schemas.adv_analytics import (
    OrderCollectionProfile,
    OrderCollectionSource,
    OrderCustomerWindow,
    OrderProductCube,
    OrderRevenueRollup,
)
from examples.orders.schemas.order import OrderFulfillment
from structure import *
from structure.platform.pyspark import *


class AdvancedOrderAnalytics(Transform):
    fulfilled = input(OrderFulfillment)
    collections = input(OrderCollectionSource)
    revenue_rollups = output(OrderRevenueRollup)
    product_cubes = output(OrderProductCube)
    customer_windows = output(OrderCustomerWindow)
    collection_profiles = output(OrderCollectionProfile)

    @step(input=fulfilled, output=revenue_rollups)
    def revenue_rollup(self, order: OrderFulfillment) -> OrderRevenueRollup:
        rollup(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            order_date=order.business.order_date,
        )

        return OrderRevenueRollup(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            order_date=order.business.order_date,
            grouping_id=grouping_id(),
            category_subtotal=is_grouped(order.product_category),
            order_count=count(),
            large_order_count=count(where=order.is_large),
            large_units=sum(order.quantity, where=order.is_large),
            any_large_order=bool_or(order.is_large),
            all_large_orders=bool_and(order.is_large),
            quantity_stddev=stddev(order.quantity),
            quantity_variance=variance(order.quantity),
            quantity_median=approx_percentile(order.quantity, 0.5, accuracy=100),
            quantity_total=sum(order.quantity),
            quantity_price_corr=corr(order.quantity, order.product_list_price),
            quantity_price_covar=covar(order.quantity, order.product_list_price),
            estimated_customers=approx_count_distinct(order.customer_id),
            first_customer_id=first_value(order.customer_id, order_by=order.quantity),
            last_customer_id=last_value(order.customer_id, order_by=order.quantity),
            customer_ids=collect_set(order.customer_id),
            order_ids=collect_list(order.id),
        )

    @step(input=fulfilled, output=product_cubes)
    def product_cube(self, order: OrderFulfillment) -> OrderProductCube:
        cube(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            customer_tier=order.customer_tier,
        )

        return OrderProductCube(
            tenant_id=order.tenant.tenant_id,
            product_category=order.product_category,
            customer_tier=order.customer_tier,
            grouping_id=grouping_id(),
            order_count=count(),
            distinct_customers=count_distinct(order.customer_id),
            gross_total=sum(order.total),
        )

    @step(input=fulfilled, output=customer_windows)
    def customer_window(self, order: OrderFulfillment) -> OrderCustomerWindow:
        customer_window = window(
            partition_by=order.customer_id,
            order_by=order.quantity,
            frame=rows_between(preceding(2), current_row()),
        )

        return OrderCustomerWindow(
            tenant_id=order.tenant.tenant_id,
            customer_id=order.customer_id,
            order_id=order.id,
            quantity=order.quantity,
            percent_rank=percent_rank(over=customer_window),
            cume_dist=cume_dist(over=customer_window),
            quantity_tile=ntile(2, over=customer_window),
            first_order_id=first_value(order.id, over=customer_window),
            last_order_id=last_value(order.id, over=customer_window),
            second_order_id=nth_value(order.id, 2, over=customer_window),
            running_units=window_sum(order.quantity, over=customer_window),
            running_avg_units=window_avg(order.quantity, over=customer_window),
            running_min_units=window_min(order.quantity, over=customer_window),
            running_max_units=window_max(order.quantity, over=customer_window),
            running_order_count=window_count(over=customer_window),
        )

    @step(input=collections, output=collection_profiles)
    def collection_profile(self, row: OrderCollectionSource) -> OrderCollectionProfile:
        normalized_attributes = map_filter(
            map_transform_keys(
                map_transform_values(row.attributes, lambda key, value: lower(trim(value))),
                lambda key, value: lower(trim(key)),
            ),
            lambda key, value: value.is_not_null(),
        )

        return OrderCollectionProfile(
            id=row.id,
            tag_count=size(row.tags),
            contains_priority=array_contains(row.tags, "priority"),
            contains_region=map_contains_key(row.extra_attributes, "Region"),
            default_tags=array("priority", "standard"),
            repeated_tags=array_repeat("priority", 2),
            all_tags=array_union(row.tags, row.extra_tags),
            tags_without_extra=array_except(row.tags, row.extra_tags),
            first_tag=element_at(row.tags, 1),
            safe_tag=try_element_at(row.tags, 2),
            region=element_at(row.extra_attributes, "Region"),
            safe_region=try_element_at(row.extra_attributes, "Region"),
            normalized_tags=arr_distinct(arr_zip_with(row.tags, row.tags, lambda left, right: lower(trim(left)))),
            sorted_tags=arr_sort_by(row.tags, lambda tag: lower(trim(tag))),
            flat_tags=arr_flatten(row.nested_tags),
            score_total=arr_aggregate(row.scores, 0, lambda acc, item: acc + item),
            tag_position=arr_position(row.tags, "priority"),
            has_priority=arr_exists(row.tags, lambda tag: lower(trim(tag)) == "priority"),
            all_tags_present=arr_forall(row.tags, lambda tag: tag.is_not_null()),
            normalized_attributes=normalized_attributes,
            zipped_attributes=map_zip_with(row.attributes, row.attributes, lambda key, left, right: lower(trim(left))),
            attribute_keys=map_keys(row.attributes),
            attribute_values=map_values(row.attributes),
            roundtrip_attributes=map_from_entries(map_entries(row.attributes)),
            merged_attributes=map_concat(row.attributes, row.extra_attributes),
        )
