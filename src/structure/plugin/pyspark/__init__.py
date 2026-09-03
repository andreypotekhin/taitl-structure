from importlib import import_module
from typing import TYPE_CHECKING, Any, cast, overload

from structure.dsl import FieldDeclaration
from structure.plugin.pyspark.dsl import field as field
from structure.plugin.pyspark.dsl import types as types
from structure.plugin.pyspark.dsl.generators import explode_array as _explode_array
from structure.plugin.pyspark.dsl.generators import explode_map as _explode_map
from structure.plugin.pyspark.dsl.generators import explode_outer_array as _explode_outer_array
from structure.plugin.pyspark.dsl.generators import explode_outer_map as _explode_outer_map
from structure.plugin.pyspark.dsl.generators import explode_outer_struct as _explode_outer_struct
from structure.plugin.pyspark.dsl.generators import explode_struct as _explode_struct
from structure.plugin.pyspark.dsl.generators import inline_outer_struct as _inline_outer_struct
from structure.plugin.pyspark.dsl.generators import inline_struct as _inline_struct
from structure.plugin.pyspark.dsl.generators import posexplode_array as _posexplode_array
from structure.plugin.pyspark.dsl.generators import posexplode_map as _posexplode_map
from structure.plugin.pyspark.dsl.generators import posexplode_outer_array as _posexplode_outer_array
from structure.plugin.pyspark.dsl.generators import posexplode_outer_map as _posexplode_outer_map
from structure.plugin.pyspark.dsl.generators import posexplode_outer_struct as _posexplode_outer_struct
from structure.plugin.pyspark.dsl.generators import posexplode_struct as _posexplode_struct
from structure.plugin.pyspark.dsl.generators import variant_explode as _variant_explode
from structure.plugin.pyspark.dsl.generators import variant_explode_outer as _variant_explode_outer
from structure.plugin.pyspark.dsl.geo import contains, geometry_as_wkt, geometry_from_wkt, intersects, within
from structure.plugin.pyspark.dsl.relation_sets import (
    except_all,
    hierarchy_closure,
    hierarchy_fallbacks,
    intersect,
    intersect_all,
    limit,
    offset,
    order_by,
    relation_alias,
    require_all,
    require_parent_hierarchy,
    require_reference,
    require_unique,
    sample,
    select_first_qualified,
    subtract,
    union_all,
    union_by_name,
)
from structure.plugin.pyspark.PySparkPlugin import PySparkPlugin


def explode_struct(*args: object, **kwargs: object) -> Any:
    return cast(Any, _explode_struct)(*args, **kwargs)


def explode_outer_struct(*args: object, **kwargs: object) -> Any:
    return cast(Any, _explode_outer_struct)(*args, **kwargs)


def explode_array(*args: object, **kwargs: object) -> Any:
    return cast(Any, _explode_array)(*args, **kwargs)


def explode_outer_array(*args: object, **kwargs: object) -> Any:
    return cast(Any, _explode_outer_array)(*args, **kwargs)


def inline_struct(*args: object, **kwargs: object) -> Any:
    return cast(Any, _inline_struct)(*args, **kwargs)


def inline_outer_struct(*args: object, **kwargs: object) -> Any:
    return cast(Any, _inline_outer_struct)(*args, **kwargs)


def posexplode_struct(*args: object, **kwargs: object) -> Any:
    return cast(Any, _posexplode_struct)(*args, **kwargs)


def posexplode_array(*args: object, **kwargs: object) -> Any:
    return cast(Any, _posexplode_array)(*args, **kwargs)


def posexplode_outer_array(*args: object, **kwargs: object) -> Any:
    return cast(Any, _posexplode_outer_array)(*args, **kwargs)


def explode_map(*args: object, **kwargs: object) -> Any:
    return cast(Any, _explode_map)(*args, **kwargs)


def explode_outer_map(*args: object, **kwargs: object) -> Any:
    return cast(Any, _explode_outer_map)(*args, **kwargs)


def posexplode_map(*args: object, **kwargs: object) -> Any:
    return cast(Any, _posexplode_map)(*args, **kwargs)


def posexplode_outer_map(*args: object, **kwargs: object) -> Any:
    return cast(Any, _posexplode_outer_map)(*args, **kwargs)


def posexplode_outer_struct(*args: object, **kwargs: object) -> Any:
    return cast(Any, _posexplode_outer_struct)(*args, **kwargs)


def variant_explode(*args: object, **kwargs: object) -> Any:
    return cast(Any, _variant_explode)(*args, **kwargs)


def variant_explode_outer(*args: object, **kwargs: object) -> Any:
    return cast(Any, _variant_explode_outer)(*args, **kwargs)


if TYPE_CHECKING:
    from structure.plugin.pyspark.api.PySpark import PySpark
    from structure.plugin.pyspark.dsl.aggregation import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.body import project, watermark, where
    from structure.plugin.pyspark.dsl.Expression import Expression
    from structure.plugin.pyspark.dsl.expressions import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.field import (  # noqa: F401
        binary,
        boolean,
        date,
        decimal,
        double,
        float,
        geometry,
        integer,
        long,
        map,
        string,
        struct,
        timestamp,
        variant,
    )
    from structure.plugin.pyspark.dsl.InputScope import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.joins import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.operations import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.operations_api import *  # type: ignore  # noqa: F403
    from structure.plugin.pyspark.dsl.TimeWindow import TimeWindow
    from structure.plugin.pyspark.dsl.types.BinaryType import BinaryType
    from structure.plugin.pyspark.dsl.types.DecimalType import DecimalType


_DSL_EXPORTS = """
AsOf BinaryType CsvOptions DecimalType Join JoinDedupe JoinHint JoinStrategy JsonOptions OverlapPolicy StreamingOutputMode TiePolicy abs acos acosh asin ascii asinh atan atan2 atanh add_months base64 bround cache checkpoint local_checkpoint persist unpersist
approx_count_distinct approx_percentile arr_aggregate arr_append arr_compact arr_distinct arr_exists arr_filter
arr_flatten arr_forall arr_position arr_prepend arr_reverse arr_insert arr_remove arr_sort arr_sort_by arr_transform
arr_zip_with array array_contains array_except array_intersect array_join array_max array_min array_repeat array_size array_union arrays_overlap avg as_of_one bool_and bool_or
collect_list collect_set concat_ws coalesce ceil char_length count count_if count_distinct corr covar cos cosh cot csc cross_join cube current_row conv date_add btrim char soundex regexp_count regexp_extract_all regexp_instr regexp_substr bit_count bit_get getbit
date_sub date_trunc date_format dayofmonth dayofweek dayofyear datediff decode cume_dist cbrt bin dedupe_earliest_by dedupe_latest_by dense_rank degrees distinct
drop_duplicates drop_duplicates_within_watermark earliest_by element_at encode event_time_between exactly_one except_all e exp expm1 factorial exists floor from_csv from_json get hash hour hypot last_day left
initcap ifnull instr intersect intersect_all first_value following full_join greatest grouping_id grouping_sets having inner_join isnan
isnotnull isnull is_grouped kurtosis lag left_join latest_by lead lookup_join last_value length levenshtein lower lpad find_in_set format_number mask overlay
ltrim ln locate log log10 log1p log2 least limit md5 map_entries map_concat map_contains_key map_filter map_from_entries map_keys map_transform_keys median
map_transform_values map_values map_zip_with max min minute mode month nanvl nvl nvl2 nullif pow not_exists nth_value
ntile offset order_by param_join percent_rank percentile pi posexplode_array posexplode_outer_array posexplode_struct posexplode_outer_struct posexplode_map posexplode_outer_map explode_array explode_outer_array explode_struct explode_outer_struct explode_map explode_outer_map inline_struct inline_outer_struct variant_explode variant_explode_outer preceding pmod project quarter rank range_between relation_alias regexp_extract regexp_replace require_all require_parent_hierarchy require_reference require_unique hierarchy_closure hierarchy_fallbacks reverse rtrim round
sample sec select_first_qualified signum sin sinh slice sha1 sha2 second rand randn radians rint right_join rollup row_number rowset_join rows_between rolling_avg rolling_max position sort_array split_part elt format_string printf substr
rolling_min rolling_sum scan subtract sum stddev sqrt size sequence session_window skewness split translate substring temporal_one next_day
to_csv to_decimal to_date to_json to_timestamp TimeWindow trim trunc try_element_at unbase64 union_all union_by_name upper unbounded_following unbounded_preceding hex unhex cardinality weekofyear
variance var_pop var_samp stddev_pop stddev_samp when width_bucket year xxhash64 zeroifnull where watermark window window_avg window_bool_and window_bool_or rpad repeat replace right sign tan tanh
window_time
window_collect_list window_collect_set window_count window_count_distinct window_max window_min window_sum
window_stddev window_variance is_valid_variant is_variant_null octet_length parse_json schema_of_csv schema_of_json schema_of_variant schema_of_variant_agg substring_index
to_variant_object try_parse_json try_variant_get variant_get
variant_literal
variant_array_append try_variant_array_append variant_insert try_variant_insert variant_set try_variant_set variant_delete
""".split()

_FIELD_FACTORIES = {
    "binary",
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
    "variant",
    "geometry",
}

__all__ = [  # noqa: F405
    "PySpark",
    "PySparkPlugin",
    "field",
    "types",
    "AsOf",
    "BinaryType",
    "CsvOptions",
    "Join",
    "JoinDedupe",
    "JoinHint",
    "JoinStrategy",
    "JsonOptions",
    "OverlapPolicy",
    "StreamingOutputMode",
    "TiePolicy",
    "abs",
    "acos",
    "acosh",
    "asin",
    "ascii",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "add_months",
    "base64",
    "bit_count",
    "bit_get",
    "bin",
    "btrim",
    "char",
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
    "array_join",
    "concat",
    "array_max",
    "array_min",
    "array_repeat",
    "array_size",
    "array_union",
    "arrays_overlap",
    "arrays_zip",
    "cardinality",
    "avg",
    "as_of_one",
    "bool_and",
    "bool_or",
    "collect_list",
    "collect_set",
    "concat_ws",
    "coalesce",
    "ceil",
    "char_length",
    "conv",
    "cos",
    "cosh",
    "cot",
    "csc",
    "count",
    "count_if",
    "count_distinct",
    "corr",
    "covar",
    "cross_join",
    "cube",
    "current_row",
    "date_add",
    "date_format",
    "date_sub",
    "date_trunc",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "datediff",
    "cbrt",
    "degrees",
    "decode",
    "elt",
    "format_string",
    "cume_dist",
    "dedupe_earliest_by",
    "dedupe_latest_by",
    "dense_rank",
    "distinct",
    "drop_duplicates",
    "drop_duplicates_within_watermark",
    "earliest_by",
    "element_at",
    "encode",
    "event_time_between",
    "exactly_one",
    "except_all",
    "e",
    "explode_struct",
    "explode_outer_struct",
    "explode_array",
    "explode_outer_array",
    "exp",
    "expm1",
    "factorial",
    "exists",
    "floor",
    "from_csv",
    "from_json",
    "get_json_object",
    "json_array_length",
    "json_object_keys",
    "schema_of_csv",
    "schema_of_json",
    "get",
    "find_in_set",
    "format_number",
    "crc32",
    "hash",
    "hex",
    "hour",
    "hypot",
    "last_day",
    "left",
    "initcap",
    "inline_struct",
    "inline_outer_struct",
    "ifnull",
    "instr",
    "intersect",
    "intersect_all",
    "first_value",
    "following",
    "full_join",
    "group_by",
    "greatest",
    "getbit",
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
    "lpad",
    "ltrim",
    "locate",
    "mask",
    "least",
    "ln",
    "log",
    "log10",
    "log1p",
    "log2",
    "limit",
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
    "median",
    "min",
    "mode",
    "minute",
    "month",
    "next_day",
    "octet_length",
    "nanvl",
    "nvl",
    "nvl2",
    "nullif",
    "pow",
    "quarter",
    "rand",
    "randn",
    "radians",
    "reduce",
    "rint",
    "not_exists",
    "nth_value",
    "ntile",
    "offset",
    "param_join",
    "order_by",
    "overlay",
    "percent_rank",
    "percentile",
    "pi",
    "position",
    "posexplode_outer_struct",
    "posexplode_struct",
    "posexplode_array",
    "posexplode_outer_array",
    "explode_map",
    "explode_outer_map",
    "posexplode_map",
    "posexplode_outer_map",
    "variant_explode",
    "variant_explode_outer",
    "preceding",
    "pmod",
    "project",
    "rank",
    "range_between",
    "relation_alias",
    "regexp_extract",
    "regexp_count",
    "regexp_extract_all",
    "regexp_instr",
    "regexp_replace",
    "regexp_substr",
    "require_all",
    "require_parent_hierarchy",
    "require_reference",
    "require_unique",
    "hierarchy_closure",
    "hierarchy_fallbacks",
    "sample",
    "select_first_qualified",
    "reverse",
    "rtrim",
    "round",
    "rpad",
    "repeat",
    "replace",
    "right",
    "sec",
    "sign",
    "signum",
    "sin",
    "sinh",
    "slice",
    "sort_array",
    "shuffle",
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
    "scan",
    "subtract",
    "sum",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "sqrt",
    "substring_index",
    "tanh",
    "size",
    "sequence",
    "session_window",
    "skewness",
    "split",
    "split_part",
    "soundex",
    "translate",
    "substring",
    "substr",
    "printf",
    "temporal_one",
    "to_csv",
    "to_decimal",
    "to_date",
    "to_json",
    "to_timestamp",
    "TimeWindow",
    "trim",
    "trunc",
    "try_element_at",
    "unbase64",
    "union_all",
    "union_by_name",
    "unhex",
    "upper",
    "unbounded_following",
    "unbounded_preceding",
    "variance",
    "var_pop",
    "var_samp",
    "weekofyear",
    "tan",
    "when",
    "year",
    "xxhash64",
    "zeroifnull",
    "where",
    "watermark",
    "width_bucket",
    "window_time",
    "is_valid_variant",
    "is_variant_null",
    "parse_json",
    "schema_of_variant",
    "schema_of_variant_agg",
    "to_variant_object",
    "try_parse_json",
    "try_variant_get",
    "variant_get",
    "variant_literal",
    "variant_array_append",
    "try_variant_array_append",
    "variant_insert",
    "try_variant_insert",
    "variant_set",
    "try_variant_set",
    "variant_delete",
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
    "binary",
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
    "variant",
    "geometry",
    "geometry_as_wkt",
    "geometry_from_wkt",
    "intersects",
    "contains",
    "within",
    "cache",
    "persist",
    "unpersist",
    "checkpoint",
    "local_checkpoint",
]


def __getattr__(name: str):
    if name == "PySpark":
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark
    if name in {"field", "types"}:
        return import_module(f"structure.plugin.pyspark.dsl.{name}")
    if name in _FIELD_FACTORIES:
        return getattr(import_module("structure.plugin.pyspark.dsl.field"), name)
    if name in {"Expression", "InputScope", "RowScope", "TimeWindow"}:
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
