from structure import Schema
from structure.platform.pyspark import field


class BitwiseSource(Schema):
    flags = field.integer(nullable=False)
    mask = field.long(nullable=False)
    label = field.string(nullable=True)
    observed = field.double(nullable=True)
    observed_fallback = field.double(nullable=True)
    recorded_on = field.date(nullable=True)
    recorded_at = field.timestamp(nullable=True)
    raw_recorded_at = field.string(nullable=True)
    tags = field.array(field.string(), contains_null=False, nullable=True)


class BitwiseProjection(Schema):
    intersected = field.integer(nullable=False)
    combined = field.long(nullable=False)
    changed = field.long(nullable=False)
    inverted = field.integer(nullable=False)
    starts_order = field.boolean(nullable=True)
    ends_hold = field.boolean(nullable=True)
    normalized_label = field.string(nullable=True)
    normalized_observed = field.double(nullable=True)
    left_trimmed_label = field.string(nullable=True)
    right_trimmed_label = field.string(nullable=True)
    rounded_observed = field.double(nullable=True)
    square_root_observed = field.double(nullable=True)
    power_observed = field.double(nullable=True)
    logarithm_observed = field.double(nullable=True)
    exponential_observed = field.double(nullable=True)
    sign_observed = field.double(nullable=True)
    previous_recorded_on = field.date(nullable=True)
    month_recorded_on = field.date(nullable=True)
    recorded_year = field.integer(nullable=True)
    recorded_hour = field.integer(nullable=True)
    parsed_recorded_on = field.date(nullable=True)
    parsed_recorded_at = field.timestamp(nullable=True)
    label_hash = field.integer(nullable=True)
    label_digest = field.string(nullable=True)
    first_two_tags = field.array(field.string(), contains_null=False, nullable=True)
    sorted_tags = field.array(field.string(), contains_null=False, nullable=True)
    tag_sequence = field.array(field.integer(), contains_null=False, nullable=False)
