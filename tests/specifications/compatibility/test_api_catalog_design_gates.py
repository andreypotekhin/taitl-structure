import inspect
from pathlib import Path

import structure.plugin.pyspark as pyspark

ROOT = Path(__file__).resolve().parents[3]
API_CATALOG = ROOT / "docs/APICatalog.md"
DESIGN = ROOT / "docs/dev/gated/ApiCatalog.gates.md"
DEFERRED = ROOT / "docs/dev/deferred/ApiCatalog.deferred.md"
SPEC = ROOT / "docs/dev/specifications/PySparkApiCatalog.spec.md"
V9_SPEC = ROOT / "docs/dev/specifications/V9ApiCatalogDesignGatedFeatures.spec.md"


def test_api_catalog_open_rows_use_design_gate_language() -> None:
    text = API_CATALOG.read_text(encoding="utf-8")

    assert "planned" not in text.lower()
    assert "| deferred |" not in text.lower()
    assert "| XML, URL, and provider/runtime functions | design-gated or unsupported |" in text
    assert "| Variant functions | partial |" in text
    assert "Variant mutation helpers remain design-gated" in text
    assert "provider-neutral geometry remains separately gated" in text
    assert "| Aggregate aliases | unsupported |" in text
    assert "| Sampling | implemented |" in text
    assert "| Join reordering | design-gated |" in text
    assert "| Nearest as-of joins | implemented |" in text
    assert "allow_missing_columns=True" in text


def test_api_catalog_design_gate_docs_cover_non_streaming_open_rows() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    deferred = DEFERRED.read_text(encoding="utf-8")
    spec = SPEC.read_text(encoding="utf-8")
    v9_spec = V9_SPEC.read_text(encoding="utf-8")
    combined = design + "\n" + deferred + "\n" + spec + "\n" + v9_spec

    for phrase in (
        "XML remains low priority",
        "Variant Mutation Profiles",
        "Geospatial Provider Boundary",
        "geometry(srid=..., nullable=True)",
        "Apache Sedona 1.9.0",
        "never the bundled PySpark plugin",
        "`GEOGRAPHY` is not admitted",
        "GeoProvider",
        "must not contain a provider name or import",
        "join_order(\"optimizer\")",
        'ties="error"',
        "sample(fraction",
        "allow_missing_columns=True",
        "no public `join_order(...)` helper",
    ):
        assert phrase in combined


def test_v9_design_gated_helpers_are_not_public_exports() -> None:
    for helper in (
        "join_order",
        "geography",
        "st_contains",
        "st_intersects",
    ):
        assert not hasattr(pyspark, helper)

    assert hasattr(pyspark, "geometry")


def test_v9_variant_helpers_are_public_exports() -> None:
    for helper in (
        "is_variant_null",
        "is_valid_variant",
        "parse_json",
        "schema_of_variant",
        "schema_of_variant_agg",
        "to_variant_object",
        "try_parse_json",
        "try_variant_get",
        "variant_literal",
        "variant_get",
        "variant_array_append",
        "try_variant_array_append",
        "variant_insert",
        "try_variant_insert",
        "variant_set",
        "try_variant_set",
        "variant_delete",
        "variant_explode",
        "variant_explode_outer",
    ):
        assert hasattr(pyspark, helper)


def test_aggregate_helpers_do_not_accept_alias_keyword() -> None:
    aggregate_helpers = (
        pyspark.approx_count_distinct,
        pyspark.approx_percentile,
        pyspark.avg,
        pyspark.bool_and,
        pyspark.bool_or,
        pyspark.collect_list,
        pyspark.collect_set,
        pyspark.corr,
        pyspark.count,
        pyspark.count_distinct,
        pyspark.covar,
        pyspark.first_value,
        pyspark.grouping_id,
        pyspark.is_grouped,
        pyspark.kurtosis,
        pyspark.last_value,
        pyspark.max,
        pyspark.min,
        pyspark.mode,
        pyspark.percentile,
        pyspark.skewness,
        pyspark.stddev,
        pyspark.sum,
        pyspark.variance,
    )

    for helper in aggregate_helpers:
        assert "alias" not in inspect.signature(helper).parameters
