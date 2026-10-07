"""Guard the pinned PySpark 4.1 source census and Structure admission ledger."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RESOURCES = ROOT / "src/structure/plugin/pyspark/resources"
DELTA = json.loads((RESOURCES / "pyspark-4.1-python-api-delta.json").read_text())
BASELINE = json.loads((RESOURCES / "pyspark-function-index-crosswalk.json").read_text())


def test_every_new_documented_python_symbol_has_one_disposition() -> None:
    """The exact 4.0 to 4.1 index differences are classified once."""
    old_functions = set(BASELINE["indexes"]["4.0.0"]["documented_function_symbols"])
    new_functions = DELTA["function_index_4_1_0"]
    function_rows = [row for row in DELTA["entries"] if row["change"] == "function-index-addition"]
    assert len(new_functions) == len(set(new_functions)) == 491
    assert old_functions <= set(new_functions)
    assert {row["symbol"].removeprefix("pyspark.sql.functions.") for row in function_rows} == (
        set(new_functions) - old_functions
    )
    assert len(function_rows) == 43

    old_sql = set(DELTA["sql_index"]["4.0.0"])
    new_sql = set(DELTA["sql_index"]["4.1.0"])
    assert len(old_sql) == 362
    assert len(new_sql) == 363
    assert new_sql - old_sql == {"pyspark.sql.Column.transform"}
    assert old_sql - new_sql == set()
    assert {row["symbol"] for row in DELTA["entries"] if row["change"] == "sql-index-addition"} == (
        new_sql - old_sql
    )


def test_each_delta_row_has_an_owner_and_honest_admission_evidence() -> None:
    """Catalog inclusion alone cannot promote a PySpark API to Structure support."""
    rows = DELTA["entries"] + DELTA["v11_carry_forward_from_4_0"]
    assert len(rows) == len({(row["symbol"], row["change"]) for row in rows})
    assert {row["structure_status"] for row in rows} <= {
        "supported", "design-gated", "caller-owned-guided"
    }
    for row in rows:
        assert row["family"] and isinstance(row["transformation"], bool)
        assert row["source_url"].startswith("https://spark.apache.org/docs/")
        assert (ROOT / row["design"]).is_file()
        assert (ROOT / row["specification"]).is_file()
        if row["structure_connect_status"] == "supported":
            assert row["symbol"] == "pyspark.sql.Column.transform"
            assert row["structure_status"] == "supported"
        assert row["streaming_status"]
        if row["structure_status"] == "supported":
            assert row["structure_spelling"]
            assert row["capability_key"] and row["diagnostic"]
            assert row["test_path"] and (ROOT / row["test_path"]).is_file()
            assert row["evidence_command"]
            assert row["coverage_id"]
            assert (ROOT / row["public_catalog_path"]).is_file()
            assert (ROOT / row["public_reference_path"]).is_file()
        else:
            assert row["reason"] and row["caller_remedy"]
            assert row["structure_spelling"] is None
            assert row["capability_key"] is None
            assert row["evidence_command"] is None


def test_v11_chronology_and_baseline_reconciliation() -> None:
    """4.0 query APIs stay in V11 scope without being mislabeled as 4.1 additions."""
    carry = {row["symbol"]: row for row in DELTA["v11_carry_forward_from_4_0"]}
    assert set(carry) == {"pyspark.sql.DataFrame.exists", "pyspark.sql.DataFrame.lateralJoin"}
    assert all(row["first_documented_version"] == "4.0.0" for row in carry.values())

    rows = {row["symbol"]: row for row in DELTA["entries"]}
    assert rows["pyspark.sql.Column.isin"]["change"] == "4.1-semantic-extension"
    assert rows["pyspark.sql.DataFrame.observe"]["change"] == "4.1-semantic-extension"
    assert rows["pyspark.sql.functions.random"]["first_documented_version"] == "1.4.0"
    assert rows["pyspark.sql.functions.try_to_date"]["first_documented_version"] == "4.0.0"
    assert not {"uniform", "randstr"} & {
        row["symbol"].removeprefix("pyspark.sql.functions.") for row in DELTA["entries"]
    }

    coverage = json.loads((RESOURCES / "pyspark-transformation-coverage.json").read_text())
    supported = [row for row in DELTA["entries"] if row["structure_status"] == "supported"]
    assert len(supported) == 1
    connect_supported = [row for row in DELTA["entries"] if row["structure_connect_status"] == "supported"]
    assert [row["symbol"] for row in connect_supported] == ["pyspark.sql.Column.transform"]
    assert any(
        entry["id"] == supported[0]["coverage_id"] and entry["status"] == "supported"
        for entry in coverage["entries"]
    )
