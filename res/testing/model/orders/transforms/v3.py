from typing import cast

from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl.Expression import Expression

from testing.model.orders.schemas.v3 import V3OrderProjection, V3OrderSource


class V3OrderFeatures(Transform):
    orders = input(V3OrderSource)
    projected = output(V3OrderProjection)

    @step(input=orders, output=projected)
    def project(self, row: V3OrderSource) -> V3OrderProjection:
        where(row.name.is_not_null(), row.raw_quantity.is_not_null())

        return V3OrderProjection(
            id=row.id,
            is_candidate=row.raw_quantity.between("1", "999"),
            name_contains_order=row.name.contains("order"),
            name_like_order=row.name.like("order%"),
            name_ilike_order=row.name.ilike("ORDER%"),
            name_matches_order=row.name.rlike("^order-[0-9]+$"),
            external_id=row.details.get_field("external_id"),
            quantity=row.raw_quantity.cast(types.integer()),
            safe_quantity=row.raw_quantity.try_cast(types.integer()),
            name_prefix=substring(row.name, start=1, length=5),
            name_words=split(row.name, pattern="\\s+"),
            name_slug=regexp_replace(row.name, pattern="\\s+", replacement="-"),
            name_digits=regexp_extract(row.name, pattern="([0-9]+)", group=1),
            name_length=length(row.name),
            name_title=initcap(row.name),
            name_reversed=reverse(row.name),
            name_translated=translate(row.name, matching="_", replacement="-"),
            order_position=instr(row.name, substring="order"),
            name_distance=levenshtein(row.name, "order"),
            display_name=concat_ws(" · ", row.name, row.details.get_field("region")),
            next_day=date_add(row.booked_on, days=1),
            days_since_booking=datediff(row.recorded_at, row.booked_on),
            booking_month=date_trunc(row.recorded_at, unit="month"),
            absolute_amount=abs(row.amount),
            rounded_amount=round(row.amount, scale=0),
            ceiling_amount=ceil(row.amount),
            floor_amount=floor(row.amount),
            score_is_nan=isnan(row.score),
            recency_rank=row_number(
                partition_by=row.details.get_field("region"),
                order_by=(cast(Expression, row.booked_on).desc_nulls_last(), row.id.asc_nulls_first()),
            ),
        )
