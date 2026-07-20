from structure import *
from structure.platform.pyspark import *


class BitwiseSource(Schema):
    flags = integer(nullable=False)
    mask = long(nullable=False)
    label = string(nullable=True)
    observed = double(nullable=True)
    observed_fallback = double(nullable=True)
    recorded_on = date(nullable=True)
    recorded_at = timestamp(nullable=True)
    raw_recorded_at = string(nullable=True)
    tags = array(string(), contains_null=False, nullable=True)


class BitwiseProjection(Schema):
    intersected = integer(nullable=False)
    combined = long(nullable=False)
    changed = long(nullable=False)
    inverted = integer(nullable=False)
    starts_order = boolean(nullable=True)
    ends_hold = boolean(nullable=True)
    normalized_label = string(nullable=True)
    normalized_observed = double(nullable=True)
    left_trimmed_label = string(nullable=True)
    right_trimmed_label = string(nullable=True)
    rounded_observed = double(nullable=True)
    square_root_observed = double(nullable=True)
    power_observed = double(nullable=True)
    logarithm_observed = double(nullable=True)
    exponential_observed = double(nullable=True)
    sign_observed = double(nullable=True)
    previous_recorded_on = date(nullable=True)
    month_recorded_on = date(nullable=True)
    recorded_year = integer(nullable=True)
    recorded_hour = integer(nullable=True)
    parsed_recorded_on = date(nullable=True)
    parsed_recorded_at = timestamp(nullable=True)
    label_hash = integer(nullable=True)
    label_digest = string(nullable=True)
    first_two_tags = array(string(), contains_null=False, nullable=True)
    sorted_tags = array(string(), contains_null=False, nullable=True)
    tag_sequence = array(integer(), contains_null=False, nullable=False)
