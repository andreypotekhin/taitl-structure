import json
from pathlib import Path
from typing import Any

import structure
from structure.plugin import pyspark

ROOT = Path(__file__).resolve().parents[3]
RESOURCES = ROOT / "src/structure/plugin/pyspark/resources"
INVENTORY = RESOURCES / "pyspark-transformation-inventory.json"
FUNCTION_INDEX_CROSSWALK = RESOURCES / "pyspark-function-index-crosswalk.json"
CATALOG = RESOURCES / "pyspark-transformation-coverage.json"
GAP_REGISTER = ROOT / "docs/dev/gaps/PySpark-SQL-baseline.gaps.md"
VALID_STATUSES = {"supported", "scheduled", "deferred", "design-gated", "unsupported", "caller-owned-guided"}


def test_pyspark_transformation_catalog_classifies_the_entire_local_inventory() -> None:
    inventory = _load(INVENTORY)
    catalog = _load(CATALOG)
    inventory_ids = [entry["id"] for entry in inventory["apis"]]
    catalog_ids = [entry["id"] for entry in catalog["entries"]]

    assert len(inventory_ids) == len(set(inventory_ids))
    assert len(catalog_ids) == len(set(catalog_ids))
    assert set(catalog_ids) == set(inventory_ids)
    assert inventory["excluded_categories"]
    extensions = {extension["name"] for extension in inventory["structure_extensions"]}
    pyspark_functions = {
        name
        for entry in inventory["apis"]
        if entry["id"].startswith("functions.")
        for name in entry["pyspark"]
    }
    assert "variant_literal" in extensions
    assert extensions.isdisjoint(pyspark_functions)


def test_every_inventory_scope_boundary_has_a_named_reason() -> None:
    inventory = _load(INVENTORY)
    for category in inventory["excluded_function_exports"]:
        assert category["id"]
        assert category["pyspark"]
        assert category["reason"]
    for category in inventory["target_only_function_exports"]:
        assert category["version"] in {"3.5.6", "4.0.0"}
        assert category["pyspark"]
        assert category["reason"]
    for module in inventory["source_modules"]:
        assert module["name"]
        assert module["version"]
        assert module["members"]
        assert module["reason"]


def test_every_selected_pyspark_function_has_exactly_one_gap_disposition() -> None:
    inventory = _load(INVENTORY)
    names = {
        name
        for entry in inventory["apis"]
        if entry["id"].startswith("functions.")
        for name in entry["pyspark"]
    }
    lines = GAP_REGISTER.read_text(encoding="utf-8").splitlines()

    for name in names:
        rows = _baseline_disposition_rows(name, lines)
        assert len(rows) == 1, f"{name} must have exactly one baseline disposition"
        row_columns, row_cells = rows[0]
        assert row_cells[0].startswith(f"`{name}(")
        assert row_cells[row_columns.index("PySpark 3.5.6")] in {"yes", "no"}, name
        assert row_cells[row_columns.index("PySpark 4.0.0")] in {"yes", "no"}, name
        structure_column = next(column for column in row_columns if column.startswith("Structure equivalent"))
        assert row_cells[row_columns.index(structure_column)], f"{name} needs a Structure contract"
        status = row_cells[row_columns.index("Status")]
        assert status in {
            "`implemented`",
            "`target-gated`",
            "`design-gated`",
            "`caller-owned-guided`",
            "`unsupported`",
            "`deferred`",
        }, f"{name} has an unrecognized disposition"
        assert row_cells[row_columns.index("Migration remedy")], f"{name} needs a migration remedy"


def test_every_common_excluded_function_has_exactly_one_gap_disposition() -> None:
    inventory = _load(INVENTORY)
    crosswalk = _load(FUNCTION_INDEX_CROSSWALK)
    lines = GAP_REGISTER.read_text(encoding="utf-8").splitlines()
    excluded = {
        name: category["id"]
        for category in inventory["excluded_function_exports"]
        for name in category["pyspark"]
    }
    expected_status = {
        "raw-expression-escape-hatches": "`unsupported`",
        "dynamic-column-and-struct-builders": "`caller-owned-guided`",
        "python-function-extensions": "`caller-owned-guided`",
        "session-and-runtime-metadata": "`caller-owned-guided`",
        "physical-execution-metadata": "`caller-owned-guided`",
        "runtime-reflection": "`caller-owned-guided`",
    }

    assert len(excluded) == 24
    for name, category in excluded.items():
        rows = _baseline_disposition_rows(name, lines)
        assert len(rows) == 1, f"{name} must have exactly one exclusion disposition"
        columns, cells = rows[0]
        assert cells[0].startswith(f"`{name}(")
        assert cells[columns.index("PySpark 3.5.6")] == "yes", name
        assert cells[columns.index("PySpark 4.0.0")] == "yes", name
        assert name in crosswalk["indexes"]["3.5.6"]["documented_function_symbols"], name
        assert name in crosswalk["indexes"]["4.0.0"]["documented_function_symbols"], name
        assert cells[columns.index("Status")] == expected_status[category], name
        contract_column = next(column for column in columns if column.startswith("Structure equivalent"))
        assert cells[columns.index(contract_column)], f"{name} needs an explicit boundary"
        assert cells[columns.index("Migration remedy")], f"{name} needs a migration remedy"


def test_selected_function_presence_matches_the_pinned_official_indexes() -> None:
    inventory = _load(INVENTORY)
    crosswalk = _load(FUNCTION_INDEX_CROSSWALK)
    lines = GAP_REGISTER.read_text(encoding="utf-8").splitlines()
    table_valued_functions = crosswalk["table_valued_functions"]

    for entry in inventory["apis"]:
        if not entry["id"].startswith("functions."):
            continue
        for inventory_name in entry["pyspark"]:
            source_name = inventory_name.removeprefix("functions.")
            if source_name.startswith("partitioning."):
                source_name = source_name.removeprefix("partitioning.")
            rows = _baseline_disposition_rows(source_name, lines)
            assert len(rows) == 1, f"{source_name} must have one baseline disposition"
            columns, cells = rows[0]

            for version in ("3.5.6", "4.0.0"):
                if source_name in {"variant_explode", "variant_explode_outer"}:
                    present = source_name in table_valued_functions[version]
                else:
                    present = source_name in crosswalk["indexes"][version]["documented_function_symbols"]
                expected = "yes" if present else "no"
                actual = cells[columns.index(f"PySpark {version}")]
                assert actual == expected, (
                    f"{source_name} PySpark {version} presence is {actual}, but the pinned official index "
                    f"records {expected}"
                )


def test_function_index_crosswalk_retains_the_pinned_source_census() -> None:
    indexes = _load(FUNCTION_INDEX_CROSSWALK)["indexes"]
    symbols = {version: set(index["documented_function_symbols"]) for version, index in indexes.items()}

    assert set(indexes) == {"3.5.6", "4.0.0"}
    assert len(symbols["3.5.6"]) == 420
    assert len(symbols["4.0.0"]) == 448
    assert len(symbols["3.5.6"] & symbols["4.0.0"]) == 409


def test_every_common_function_export_is_selected_or_has_an_explicit_exclusion() -> None:
    inventory = _load(INVENTORY)
    indexes = _load(FUNCTION_INDEX_CROSSWALK)["indexes"]
    common = set(indexes["3.5.6"]["documented_function_symbols"]) & set(
        indexes["4.0.0"]["documented_function_symbols"]
    )
    selected = {
        name.removeprefix("functions.partitioning.")
        for entry in inventory["apis"]
        for name in entry["pyspark"]
    }
    exclusions = [
        name for category in inventory["excluded_function_exports"] for name in category["pyspark"]
    ]

    assert len(exclusions) == len(set(exclusions))
    assert selected.isdisjoint(exclusions)
    assert (selected & common) | set(exclusions) == common


def test_every_function_index_export_has_a_scope_disposition() -> None:
    inventory = _load(INVENTORY)
    indexes = _load(FUNCTION_INDEX_CROSSWALK)["indexes"]
    all_exports = {
        version: set(index["documented_function_symbols"])
        for version, index in indexes.items()
    }
    selected = {
        name.removeprefix("functions.partitioning.")
        for entry in inventory["apis"]
        for name in entry["pyspark"]
    }
    excluded = {
        name for category in inventory["excluded_function_exports"] for name in category["pyspark"]
    }
    target_only = {
        name: entry["version"]
        for entry in inventory["target_only_function_exports"]
        for name in entry["pyspark"]
    }
    modules = {
        (entry["version"], entry["name"]): set(entry["members"])
        for entry in inventory["source_modules"]
    }

    assert set(target_only).isdisjoint(selected | excluded)
    for name, version in target_only.items():
        assert name in all_exports[version]
        other_version = "3.5.6" if version == "4.0.0" else "4.0.0"
        assert name not in all_exports[other_version]
    for version, exports in all_exports.items():
        accounted = (selected | excluded) & exports
        accounted |= {name for name, source_version in target_only.items() if source_version == version}
        accounted |= {name for module_version, name in modules if module_version == version}
        assert exports == accounted


def test_target_only_function_exports_have_exact_gap_remedies() -> None:
    inventory = _load(INVENTORY)
    source_rows = {
        name: entry["version"]
        for entry in inventory["target_only_function_exports"]
        for name in entry["pyspark"]
    }
    lines = GAP_REGISTER.read_text(encoding="utf-8").splitlines()
    target_section = False
    target_rows: dict[str, tuple[str, str, str]] = {}
    for line in lines:
        if line.startswith("## Target-Line Additions Outside the Default Baseline"):
            target_section = True
            continue
        if target_section and line.startswith("## "):
            break
        if not target_section or not line.startswith("| `"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        name = cells[0].removeprefix("`").removesuffix("`")
        assert name not in target_rows, f"duplicate target-only disposition for {name}"
        target_rows[name] = (cells[1], cells[2], cells[3])

    assert set(source_rows).issubset(target_rows)
    for name, version in source_rows.items():
        row_version, status, remedy = target_rows[name]
        assert row_version == version
        assert status == "`target-gated`"
        assert remedy


def test_pyspark_transformation_catalog_entries_are_actionable() -> None:
    for entry in _load(CATALOG)["entries"]:
        assert entry["status"] in VALID_STATUSES
        assert entry["structure"]
        assert entry["profile"]
        assert entry["contract"]
        assert entry["notes"]
        assert entry["evidence"]
        assert "4.1" not in entry["profile"]
        for evidence in entry["evidence"]:
            assert (ROOT / evidence).is_file(), f"{entry['id']} evidence is missing: {evidence}"


def test_supported_catalog_entries_name_exported_structure_api_evidence() -> None:
    for entry in _load(CATALOG)["entries"]:
        if entry["status"] != "supported":
            continue
        assert entry["public_symbols"], f"{entry['id']} lacks a public Structure spelling"
        assert all(hasattr(structure, symbol) or hasattr(pyspark, symbol) for symbol in entry["public_symbols"])
        assert any(
            "tests/" in evidence for evidence in entry["evidence"]
        ), f"{entry['id']} lacks parity or generated-code evidence"


def test_array_lookup_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.array-lookup", expected_status="implemented")


def test_array_construction_functions_have_exactly_one_baseline_disposition() -> None:
    statuses = {
        "array": "implemented",
        "array_repeat": "implemented",
        "sequence": "caller-owned-guided",
        "array_append": "implemented",
        "array_prepend": "implemented",
        "array_insert": "implemented",
        "array_remove": "caller-owned-guided",
        "array_compact": "implemented",
    }
    for function, status in statuses.items():
        _assert_baseline_function_rows("functions.array-construction", status, function)


def test_array_composition_and_ordering_functions_have_exactly_one_disposition() -> None:
    _assert_baseline_function_rows("functions.array-advanced", "implemented")


def test_array_transform_functions_have_exactly_one_baseline_disposition() -> None:
    statuses = {
        "array_distinct": "implemented",
        "array_union": "implemented",
        "array_intersect": "implemented",
        "array_except": "implemented",
        "array_sort": "caller-owned-guided",
        "reverse": "implemented",
        "flatten": "implemented",
        "transform": "implemented",
        "filter": "implemented",
        "exists": "implemented",
        "forall": "implemented",
        "aggregate": "implemented",
        "reduce": "implemented",
        "zip_with": "implemented",
        "arrays_zip": "caller-owned-guided",
    }
    for function, status in statuses.items():
        _assert_baseline_function_rows("functions.array-transform", status, function)


def test_temporal_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.temporal", "implemented")


def test_query_clock_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.query-clock", "implemented")


def test_advanced_aggregate_functions_have_exactly_one_baseline_disposition() -> None:
    statuses = {
        "count_distinct": "caller-owned-guided",
        "count_if": "implemented",
        "approx_count_distinct": "implemented",
        "approx_percentile": "caller-owned-guided",
        "percentile_approx": "implemented",
        "median": "implemented",
        "percentile": "caller-owned-guided",
        "std": "implemented",
        "stddev": "implemented",
        "stddev_pop": "implemented",
        "stddev_samp": "implemented",
        "variance": "implemented",
        "var_pop": "implemented",
        "var_samp": "implemented",
        "corr": "implemented",
        "covar_pop": "implemented",
        "covar_samp": "implemented",
        "histogram_numeric": "implemented",
        "skewness": "implemented",
        "kurtosis": "implemented",
        "mode": "implemented",
        "any_value": "implemented",
        "array_agg": "implemented",
        "bit_and": "implemented",
        "bit_or": "implemented",
        "bit_xor": "implemented",
        "bool_and": "implemented",
        "bool_or": "implemented",
        "some": "implemented",
        "every": "implemented",
        "first": "implemented",
        "last": "implemented",
        "max_by": "implemented",
        "min_by": "implemented",
        "mean": "implemented",
        "product": "implemented",
        "sum_distinct": "implemented",
        "regr_avgx": "implemented",
        "regr_avgy": "implemented",
        "regr_count": "implemented",
        "regr_intercept": "implemented",
        "regr_r2": "implemented",
        "regr_slope": "implemented",
        "regr_sxx": "implemented",
        "regr_sxy": "implemented",
        "regr_syy": "implemented",
    }
    for function, status in statuses.items():
        _assert_baseline_function_rows("functions.aggregate-advanced", status, function)


def test_core_aggregate_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.aggregate-core", "implemented")


def test_hash_function_exports_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.hash", "implemented")


def test_function_constructors_and_relation_hints_have_dispositions() -> None:
    _assert_baseline_function_rows("functions.literal", "implemented")
    _assert_baseline_function_rows("functions.relation-hint", "caller-owned-guided")


def test_time_window_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.time-window", "implemented")


def test_xml_functions_remain_explicitly_design_gated() -> None:
    _assert_baseline_function_rows("functions.xml", "design-gated")


def test_window_functions_have_exactly_one_baseline_disposition() -> None:
    statuses = {
        "row_number": "implemented",
        "rank": "implemented",
        "dense_rank": "implemented",
        "percent_rank": "implemented",
        "cume_dist": "implemented",
        "ntile": "implemented",
        "lag": "caller-owned-guided",
        "lead": "caller-owned-guided",
        "first_value": "caller-owned-guided",
        "last_value": "caller-owned-guided",
        "nth_value": "caller-owned-guided",
    }
    for function, status in statuses.items():
        _assert_baseline_function_rows("functions.window", status, function)


def test_generator_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.generators", expected_status="implemented")


def test_stack_has_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.stack", expected_status="implemented")


def test_map_and_struct_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.map", expected_status="implemented")


def test_json_and_csv_functions_have_exactly_one_baseline_disposition() -> None:
    statuses = {
        "from_json": "caller-owned-guided",
        "to_json": "implemented",
        "from_csv": "implemented",
        "to_csv": "implemented",
        "get_json_object": "implemented",
        "json_array_length": "implemented",
        "json_object_keys": "implemented",
        "json_tuple": "implemented",
        "schema_of_json": "implemented",
        "schema_of_csv": "implemented",
    }
    for function, status in statuses.items():
        _assert_baseline_function_rows("functions.parsing", status, function)


def test_sketch_and_bitmap_functions_have_exactly_one_baseline_disposition() -> None:
    statuses = {
        "hll_sketch_agg": "implemented",
        "hll_union_agg": "implemented",
        "hll_union": "implemented",
        "hll_sketch_estimate": "implemented",
        "bitmap_bit_position": "implemented",
        "bitmap_bucket_number": "implemented",
        "bitmap_construct_agg": "implemented",
        "bitmap_or_agg": "implemented",
        "bitmap_count": "implemented",
        "count_min_sketch": "caller-owned-guided",
    }
    for function, status in statuses.items():
        _assert_baseline_function_rows("functions.sketches", status, function)


def test_random_functions_have_exactly_one_baseline_disposition() -> None:
    _assert_baseline_function_rows("functions.random", "implemented")


def test_try_url_decode_has_an_explicit_target_line_gate() -> None:
    _assert_baseline_function_rows(
        "functions.string", "target-gated", "try_url_decode", expected_presence=("no", "yes")
    )


def test_unresolved_numeric_and_specialized_string_boundaries_are_precisely_gated() -> None:
    for function in (
        "try_add",
        "try_divide",
        "try_multiply",
        "try_subtract",
        "try_avg",
        "try_sum",
        "parse_url",
        "sentences",
        "to_char",
        "to_number",
        "to_varchar",
        "try_to_number",
    ):
        rows = _baseline_disposition_rows(function, GAP_REGISTER.read_text(encoding="utf-8").splitlines())
        assert len(rows) == 1, f"{function} must have one boundary disposition"
        columns, cells = rows[0]
        assert cells[columns.index("Status")] == "`design-gated`", function
        assert cells[columns.index("Migration remedy")], function


def test_variant_functions_and_tvfs_have_exactly_one_target_disposition() -> None:
    target_40 = {
        "parse_json",
        "try_parse_json",
        "schema_of_variant",
        "schema_of_variant_agg",
        "variant_get",
        "try_variant_get",
        "to_variant_object",
        "is_variant_null",
        "variant_explode",
        "variant_explode_outer",
    }
    for function in target_40:
        _assert_baseline_function_rows(
            "functions.variant", "target-gated", function, expected_presence=("no", "yes")
        )
    _assert_baseline_function_rows(
        "functions.variant", "target-gated", "is_valid_variant", expected_presence=("no", "no")
    )


def test_variant_mutations_remain_outside_reviewed_target_lines() -> None:
    _assert_baseline_function_rows(
        "functions.variant-mutation", "target-gated", expected_presence=("no", "no")
    )


def _assert_baseline_function_rows(
    inventory_id: str,
    expected_status: str,
    function: str | None = None,
    expected_presence: tuple[str, str] = ("yes", "yes"),
) -> None:
    inventory = _load(INVENTORY)
    names = next(entry["pyspark"] for entry in inventory["apis"] if entry["id"] == inventory_id)
    if function is not None:
        assert function in names, f"{function} is not in {inventory_id}"
        names = [function]
    lines = GAP_REGISTER.read_text(encoding="utf-8").splitlines()

    for name in names:
        rows = _baseline_disposition_rows(name, lines)
        assert len(rows) == 1, f"{name} must have one per-function baseline disposition"
        columns, cells = rows[0]
        assert cells[columns.index("PySpark 3.5.6")] == expected_presence[0], (
            f"{name} 3.5.6 presence is not reconciled"
        )
        assert cells[columns.index("PySpark 4.0.0")] == expected_presence[1], (
            f"{name} 4.0.0 presence is not reconciled"
        )
        assert cells[columns.index("Status")] == f"`{expected_status}`", (
            f"{name} needs an explicit {expected_status} disposition"
        )


def _baseline_disposition_rows(name: str, lines: list[str]) -> list[tuple[list[str], list[str]]]:
    rows: list[tuple[list[str], list[str]]] = []
    columns: list[str] | None = None
    for line in lines:
        if line.startswith("| PySpark function"):
            header = [cell.strip() for cell in line.strip("|").split("|")]
            columns = header if {"PySpark 3.5.6", "PySpark 4.0.0"}.issubset(header) else None
            continue
        if not line.startswith("|"):
            columns = None
            continue
        if columns is None or line.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells and (cells[0].startswith(f"`{name}(") or cells[0] == f"`{name}`"):
            rows.append((columns, cells))
    return rows


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
