from importlib import import_module
from typing import TYPE_CHECKING, Any, overload

from structure.dsl import FieldDeclaration
from structure.plugin.pyspark.PySparkPlugin import PySparkPlugin

if TYPE_CHECKING:
    from structure.plugin.pyspark.api.PySpark import PySpark
    from structure.plugin.pyspark.dsl import types
    from structure.plugin.pyspark.dsl.Expression import Expression
    from structure.plugin.pyspark.dsl.InputScope import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.Projection import Projection
    from structure.plugin.pyspark.dsl.TimeWindow import TimeWindow
    from structure.plugin.pyspark.dsl.aggregation import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.body import project, watermark, where
    from structure.plugin.pyspark.dsl.expressions import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.field import (  # noqa: F401
        boolean,
        date,
        decimal,
        double,
        float,
        integer,
        long,
        map,
        string,
        struct,
        timestamp,
    )
    from structure.plugin.pyspark.dsl.joins import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.operations import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.operations_api import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.types.DecimalType import DecimalType


_DSL_EXPORTS = """
AsOf DecimalType Join JoinDedupe JoinHint JoinStrategy OverlapPolicy StreamingOutputMode TiePolicy abs bround
approx_count_distinct approx_percentile arr_aggregate arr_append arr_compact arr_distinct arr_exists arr_filter
arr_flatten arr_forall arr_position arr_prepend arr_reverse arr_insert arr_remove arr_sort arr_sort_by arr_transform
arr_zip_with array array_contains array_except array_intersect array_repeat array_union avg as_of_one bool_and bool_or
collect_list collect_set concat_ws coalesce ceil count count_distinct corr covar cross_join cube current_row date_add
date_sub date_trunc dayofmonth datediff cume_dist dedupe_earliest_by dedupe_latest_by dense_rank distinct
drop_duplicates drop_duplicates_within_watermark earliest_by element_at event_time_between exactly_one exp exists floor hash hour
initcap ifnull instr first_value following full_join group_by grouping_id grouping_sets having inner_join isnan
isnotnull isnull is_grouped kurtosis lag left_join latest_by lead lookup_join last_value length levenshtein lower
ltrim log md5 map_entries map_concat map_contains_key map_filter map_from_entries map_keys map_transform_keys
map_transform_values map_values map_zip_with max min minute month nanvl nvl nvl2 nullif pow not_exists nth_value
ntile percent_rank percentile preceding project rank range_between regexp_extract regexp_replace reverse rtrim round
signum slice sha1 sha2 second right_join rollup row_number rowset_join rows_between rolling_avg rolling_max
rolling_min rolling_sum sum stddev sqrt size sequence session_window skewness split translate substring temporal_one
to_decimal to_date to_timestamp TimeWindow trim trunc try_element_at upper unbounded_following unbounded_preceding
variance when year xxhash64 zeroifnull where watermark window window_avg window_bool_and window_bool_or
window_collect_list window_collect_set window_count window_count_distinct window_max window_min window_sum
window_stddev window_variance
""".split()

_FIELD_FACTORIES = {
    "boolean",
    "date",
    "decimal",
    "double",
    "float",
    "integer",
    "long",
    "map",
    "string",
    "struct",
    "timestamp",
}

__all__ = [  # noqa: F405
    "PySpark",
    "PySparkPlugin",
    "field",
    "types",
    "AsOf",
    "Join",
    "JoinDedupe",
    "JoinHint",
    "JoinStrategy",
    "OverlapPolicy",
    "StreamingOutputMode",
    "TiePolicy",
    "abs",
    "bround",
    "approx_count_distinct",
    "approx_percentile",
    "arr_aggregate",
    "arr_append",
    "arr_compact",
    "arr_distinct",
    "arr_exists",
    "arr_filter",
    "arr_flatten",
    "arr_forall",
    "arr_position",
    "arr_prepend",
    "arr_reverse",
    "arr_insert",
    "arr_remove",
    "arr_sort",
    "arr_sort_by",
    "arr_transform",
    "arr_zip_with",
    "array",
    "array_contains",
    "array_except",
    "array_intersect",
    "array_repeat",
    "array_union",
    "avg",
    "as_of_one",
    "bool_and",
    "bool_or",
    "collect_list",
    "collect_set",
    "concat_ws",
    "coalesce",
    "ceil",
    "count",
    "count_distinct",
    "corr",
    "covar",
    "cross_join",
    "cube",
    "current_row",
    "date_add",
    "date_sub",
    "date_trunc",
    "dayofmonth",
    "datediff",
    "cume_dist",
    "dedupe_earliest_by",
    "dedupe_latest_by",
    "dense_rank",
    "distinct",
    "drop_duplicates",
    "drop_duplicates_within_watermark",
    "earliest_by",
    "element_at",
    "event_time_between",
    "exactly_one",
    "exp",
    "exists",
    "floor",
    "hash",
    "hour",
    "initcap",
    "ifnull",
    "instr",
    "first_value",
    "following",
    "full_join",
    "group_by",
    "grouping_id",
    "grouping_sets",
    "having",
    "inner_join",
    "isnan",
    "isnotnull",
    "isnull",
    "is_grouped",
    "kurtosis",
    "lag",
    "left_join",
    "latest_by",
    "lead",
    "lookup_join",
    "last_value",
    "length",
    "levenshtein",
    "lower",
    "ltrim",
    "log",
    "md5",
    "map_entries",
    "map_concat",
    "map_contains_key",
    "map_filter",
    "map_from_entries",
    "map_keys",
    "map_transform_keys",
    "map_transform_values",
    "map_values",
    "map_zip_with",
    "max",
    "min",
    "minute",
    "month",
    "nanvl",
    "nvl",
    "nvl2",
    "nullif",
    "pow",
    "not_exists",
    "nth_value",
    "ntile",
    "percent_rank",
    "percentile",
    "preceding",
    "project",
    "rank",
    "range_between",
    "regexp_extract",
    "regexp_replace",
    "reverse",
    "rtrim",
    "round",
    "signum",
    "slice",
    "sha1",
    "sha2",
    "second",
    "right_join",
    "rollup",
    "row_number",
    "rowset_join",
    "rows_between",
    "rolling_avg",
    "rolling_max",
    "rolling_min",
    "rolling_sum",
    "sum",
    "stddev",
    "sqrt",
    "size",
    "sequence",
    "session_window",
    "skewness",
    "split",
    "translate",
    "substring",
    "temporal_one",
    "to_decimal",
    "to_date",
    "to_timestamp",
    "TimeWindow",
    "trim",
    "trunc",
    "try_element_at",
    "upper",
    "unbounded_following",
    "unbounded_preceding",
    "variance",
    "when",
    "year",
    "xxhash64",
    "zeroifnull",
    "where",
    "watermark",
    "window",
    "window_avg",
    "window_bool_and",
    "window_bool_or",
    "window_collect_list",
    "window_collect_set",
    "window_count",
    "window_count_distinct",
    "window_max",
    "window_min",
    "window_sum",
    "window_stddev",
    "window_variance",
    "boolean",
    "date",
    "decimal",
    "double",
    "float",
    "integer",
    "long",
    "map",
    "string",
    "struct",
    "timestamp",
]


def __getattr__(name: str):
    if name == "PySpark":
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark
    if name in {"field", "types"}:
        return import_module(f"structure.plugin.pyspark.dsl.{name}")
    if name in _FIELD_FACTORIES:
        return getattr(import_module("structure.plugin.pyspark.dsl.field"), name)
    if name in {"Expression", "InputScope", "Projection", "RowScope", "TimeWindow"}:
        return getattr(import_module(f"structure.plugin.pyspark.dsl.{name}"), name)
    dsl = import_module("structure.plugin.pyspark.dsl")

    try:
        return getattr(dsl, name)
    except AttributeError as error:
        raise AttributeError(name) from error


def _array(*values: object, **options: object):
    """Create a schema array field or a symbolic array expression by argument shape."""
    if len(values) == 1 and isinstance(values[0], FieldDeclaration):
        return import_module("structure.plugin.pyspark.dsl.field").array(values[0], **options)
    return import_module("structure.plugin.pyspark.dsl.operations_api").array(*values, **options)


@overload  # type: ignore[no-redef]
def array(
    element: FieldDeclaration,
    *,
    contains_null: bool = True,
    nullable: bool = True,
    alias: str | None = None,
    metadata: object | None = None,
    description: str | None = None,
) -> Any: ...


@overload
def array(*values: object, **options: object) -> "Expression": ...


def array(*values: object, **options: object):
    return _array(*values, **options)
