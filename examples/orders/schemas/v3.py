from structure import Schema
from structure.platform.pyspark.dsl.field import *


class V3OrderDetails(Schema):
    external_id = string(nullable=True)
    region = string(nullable=True)


class V3OrderSource(Schema):
    id = string(nullable=False)
    name = string(nullable=True)
    raw_quantity = string(nullable=True)
    amount = decimal(11, 2, nullable=True)
    score = double(nullable=True)
    booked_on = date(nullable=True)
    recorded_at = timestamp(nullable=True)
    details = struct(V3OrderDetails, nullable=True)


class V3OrderProjection(Schema):
    id = string(nullable=False)
    is_candidate = boolean(nullable=True)
    name_contains_order = boolean(nullable=True)
    name_like_order = boolean(nullable=True)
    name_ilike_order = boolean(nullable=True)
    name_matches_order = boolean(nullable=True)
    external_id = string(nullable=True)
    quantity = integer(nullable=True)
    safe_quantity = integer(nullable=True)
    name_prefix = string(nullable=True)
    name_words = array(string(), contains_null=False, nullable=True)
    name_slug = string(nullable=True)
    name_digits = string(nullable=True)
    name_length = integer(nullable=True)
    name_title = string(nullable=True)
    name_reversed = string(nullable=True)
    name_translated = string(nullable=True)
    order_position = integer(nullable=True)
    name_distance = integer(nullable=True)
    display_name = string(nullable=False)
    next_day = date(nullable=True)
    days_since_booking = integer(nullable=True)
    booking_month = timestamp(nullable=True)
    absolute_amount = decimal(11, 2, nullable=True)
    rounded_amount = decimal(10, 0, nullable=True)
    ceiling_amount = decimal(11, 0, nullable=True)
    floor_amount = decimal(11, 0, nullable=True)
    score_is_nan = boolean(nullable=False)
    recency_rank = long(nullable=True)
