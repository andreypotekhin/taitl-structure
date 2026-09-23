def ordered_aggregate_guard(order, *, latest: bool, name: str, functions):
    """Check the winning ordering value within the aggregate, including streaming groups."""
    values = functions.collect_list(order)
    extreme = functions.array_max(values) if latest else functions.array_min(values)
    winners = functions.filter(values, lambda value: value == extreme)
    message = (
        f"{name}(ties='error') found tied selected values; make the ordering value unique "
        "within each group; see docs/reference/Aggregations.ref.md"
    )
    return functions.assert_true(functions.size(winners) <= functions.lit(1), message)
