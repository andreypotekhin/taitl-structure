from structure import *


class V3OrderDetails(Schema):
    external_id = field(String(), nullable=True)
    region = field(String(), nullable=True)


class V3OrderSource(Schema):
    id = field(String(), nullable=False)
    name = field(String(), nullable=True)
    raw_quantity = field(String(), nullable=True)
    amount = field(Decimal(11, 2), nullable=True)
    score = field(Double(), nullable=True)
    booked_on = field(Date(), nullable=True)
    recorded_at = field(Timestamp(), nullable=True)
    details = field(Struct(V3OrderDetails), nullable=True)


class V3OrderProjection(Schema):
    id = field(String(), nullable=False)
    is_candidate = field(Boolean(), nullable=True)
    name_contains_order = field(Boolean(), nullable=True)
    name_like_order = field(Boolean(), nullable=True)
    name_ilike_order = field(Boolean(), nullable=True)
    name_matches_order = field(Boolean(), nullable=True)
    external_id = field(String(), nullable=True)
    quantity = field(Integer(), nullable=True)
    safe_quantity = field(Integer(), nullable=True)
    name_prefix = field(String(), nullable=True)
    name_words = field(Array(String(), contains_null=False), nullable=True)
    name_slug = field(String(), nullable=True)
    name_digits = field(String(), nullable=True)
    name_length = field(Integer(), nullable=True)
    name_title = field(String(), nullable=True)
    name_reversed = field(String(), nullable=True)
    name_translated = field(String(), nullable=True)
    order_position = field(Integer(), nullable=True)
    name_distance = field(Integer(), nullable=True)
    display_name = field(String(), nullable=False)
    next_day = field(Date(), nullable=True)
    days_since_booking = field(Integer(), nullable=True)
    booking_month = field(Timestamp(), nullable=True)
    absolute_amount = field(Decimal(11, 2), nullable=True)
    rounded_amount = field(Decimal(11, 2), nullable=True)
    ceiling_amount = field(Decimal(11, 0), nullable=True)
    floor_amount = field(Decimal(11, 0), nullable=True)
    score_is_nan = field(Boolean(), nullable=False)
    recency_rank = field(Long(), nullable=True)
