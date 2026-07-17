from structure import (
    Transform,
    arr_sort,
    bround,
    date_sub,
    exp,
    hash,
    hour,
    input,
    log,
    ltrim,
    nanvl,
    nullif,
    output,
    pow,
    rtrim,
    sequence,
    signum,
    sha2,
    slice,
    sqrt,
    to_date,
    to_timestamp,
    transform,
    trunc,
    year,
)
from testing.model.v4.orders.schemas.scalar import BitwiseProjection, BitwiseSource


@transform
class BitwiseFeatures(Transform):
    source = input(BitwiseSource)
    target = output(BitwiseProjection)

    def project(self, row: BitwiseSource) -> BitwiseProjection:
        return BitwiseProjection(
            intersected=row.flags.bitwise_and(3),
            combined=row.flags.bitwise_or(row.mask),
            changed=row.flags.bitwise_xor(row.mask),
            inverted=row.flags.bitwise_not(),
            starts_order=row.label.startswith("order-"),
            ends_hold=row.label.endswith("-hold"),
            normalized_label=nullif(row.label, "unknown"),
            normalized_observed=nanvl(row.observed, row.observed_fallback),
            left_trimmed_label=ltrim(row.label),
            right_trimmed_label=rtrim(row.label),
            rounded_observed=bround(row.observed, scale=1),
            square_root_observed=sqrt(row.observed),
            power_observed=pow(row.observed, 2),
            logarithm_observed=log(row.observed, base=10),
            exponential_observed=exp(row.observed),
            sign_observed=signum(row.observed),
            previous_recorded_on=date_sub(row.recorded_on, days=1),
            month_recorded_on=trunc(row.recorded_on, unit="month"),
            recorded_year=year(row.recorded_on),
            recorded_hour=hour(row.recorded_at),
            parsed_recorded_on=to_date(row.raw_recorded_at, format="yyyy-MM-dd HH:mm:ss"),
            parsed_recorded_at=to_timestamp(row.raw_recorded_at, format="yyyy-MM-dd HH:mm:ss"),
            label_hash=hash(row.label, row.flags),
            label_digest=sha2(row.label, bits=256),
            first_two_tags=slice(row.tags, 1, 2),
            sorted_tags=arr_sort(row.tags),
            tag_sequence=sequence(1, 3),
        )
