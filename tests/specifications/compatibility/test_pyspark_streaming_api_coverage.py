import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
RESOURCES = ROOT / "src/structure/plugin/pyspark/resources"
V8_LEDGER = RESOURCES / "pyspark-structured-streaming-coverage.json"
V9_LEDGER = RESOURCES / "pyspark-streaming-api-coverage.json"
API_CATALOG = ROOT / "docs/APICatalog.md"
VALID_STATUSES = {
    "structure-supported",
    "caller-owned-guided",
    "design-gated",
    "streaming-ineligible",
    "out-of-scope",
}
STRUCTURE_SUPPORTED = {"structure-supported"}
V8_SUPPORTED = {"streaming-supported", "streaming-partial"}
LIFECYCLE_IDS = {
    "streaming.sources",
    "streaming.sinks",
    "streaming.output-modes",
    "streaming.checkpoints",
    "streaming.triggers",
    "streaming.query-lifecycle",
}
REQUIRED_V9_IDS = LIFECYCLE_IDS | {
    "streaming.input-declarations",
    "streaming.dataframe-metadata",
    "streaming.event-time-bounds",
    "streaming.stateful-composition",
    "streaming.chained-window-aggregation",
    "streaming.chained-stateful-operators",
    "streaming.distinct-style-sets",
    "streaming.ordering-bounds",
    "streaming.priority-selection",
    "streaming.selected-row-helpers",
    "streaming.analytic-windows",
    "streaming.foreach-batch",
    "streaming.foreach",
    "streaming.listeners",
    "streaming.arbitrary-state",
    "streaming.rdd-pandas-boundaries",
    "streaming.actions",
    "streaming.spark-connect",
}


def test_streaming_api_ledger_classifies_every_selected_v9_family() -> None:
    entries = _v9_entries()
    ids = [entry["id"] for entry in entries]

    assert len(ids) == len(set(ids))
    assert REQUIRED_V9_IDS <= set(ids)


def test_streaming_api_ledger_inherits_v8_transformation_classifications() -> None:
    v8_entries = {entry["id"]: entry for entry in _load(V8_LEDGER)["entries"]}
    v9_entries = {entry["id"]: entry for entry in _v9_entries()}

    assert set(v8_entries) <= set(v9_entries)
    for api_id, v8_entry in v8_entries.items():
        v9_entry = v9_entries[api_id]
        expected = "structure-supported" if v8_entry["status"] in V8_SUPPORTED else "streaming-ineligible"

        assert v9_entry["status"] == expected, api_id


def test_streaming_api_ledger_entries_are_actionable() -> None:
    for entry in _v9_entries():
        assert entry["status"] in VALID_STATUSES
        assert entry["api_family"]
        assert entry["owner_boundary"]
        assert entry["support_claim"]
        assert entry["pyspark_apis"]
        assert entry["structure_surface"]
        assert entry["evidence"]
        assert entry["notes"]
        for evidence in entry["evidence"]:
            assert (ROOT / evidence).is_file(), f"{entry['id']} evidence is missing: {evidence}"


def test_lifecycle_apis_are_guided_without_becoming_structure_transform_claims() -> None:
    entries = {entry["id"]: entry for entry in _v9_entries()}

    for api_id in LIFECYCLE_IDS:
        entry = entries[api_id]

        assert entry["status"] == "caller-owned-guided"
        assert entry["owner_boundary"] == "caller-owned"
        assert entry["support_claim"] == "adoption-guidance"
        assert "lifecycle" in entry["api_family"]


def test_foreach_batch_is_guided_without_admitting_row_foreach() -> None:
    entries = {entry["id"]: entry for entry in _v9_entries()}

    foreach_batch = entries["streaming.foreach-batch"]
    assert foreach_batch["status"] == "caller-owned-guided"
    assert foreach_batch["owner_boundary"] == "caller-owned"
    assert foreach_batch["support_claim"] == "adoption-guidance"
    assert "start_foreach_batch_query" in " ".join(foreach_batch["structure_surface"])

    foreach = entries["streaming.foreach"]
    assert foreach["status"] == "design-gated"
    assert foreach["support_claim"] == "no-structure-support"
    assert "DataStreamWriter.foreach" in foreach["pyspark_apis"]


def test_chained_window_aggregation_has_structure_and_live_profile_evidence() -> None:
    entry = {entry["id"]: entry for entry in _v9_entries()}["streaming.chained-window-aggregation"]

    assert entry["status"] == "structure-supported"
    assert entry["owner_boundary"] == "structure-transform"
    assert "window_time" in entry["structure_surface"]
    assert "tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py" in entry["evidence"]
    assert "tests/integration/pyspark/v3/streams/test_file_streams.py" in entry["evidence"]
    assert "PySpark 3.5 and 4.0" in entry["notes"]


def test_target_gated_variant_streaming_claim_has_profile_evidence() -> None:
    entry = {entry["id"]: entry for entry in _v9_entries()}["functions.variant"]

    assert entry["status"] == "structure-supported"
    assert "tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py" in entry["evidence"]
    assert "tests/integration/pyspark/v3/streams/test_file_streams.py" in entry["evidence"]
    assert "PySpark 4.0 streaming evidence" in entry["notes"]
    assert "PySpark 3.5 evidence proves target rejection" in entry["notes"]
    assert "PySpark 4.2 profile" in entry["notes"]


def test_structure_supported_streaming_apis_do_not_claim_lifecycle_ownership() -> None:
    lifecycle_tokens = ("readStream", "writeStream", "start()", "awaitTermination", "checkpointLocation", "trigger(")

    for entry in _v9_entries():
        if entry["status"] not in STRUCTURE_SUPPORTED:
            continue

        assert entry["owner_boundary"] == "structure-transform"
        assert "lifecycle" not in entry["api_family"]
        assert not any(token in " ".join(entry["structure_surface"]) for token in lifecycle_tokens)


def test_public_streaming_catalog_uses_v9_status_language() -> None:
    streaming = _section(API_CATALOG.read_text(encoding="utf-8"), "## Streaming", "## API Coverage")

    assert "| planned |" not in streaming
    assert "| deferred |" not in streaming
    assert "| Typed struct generators | implemented |" in streaming
    assert "| Chained stateful operators | design-gated |" in streaming
    assert "| Global ordering, limits, and offsets | streaming-ineligible |" in streaming
    assert "| Analytic windows and selected-row helpers | streaming-ineligible |" in streaming
    assert "| `foreachBatch` side-effect sinks | caller-owned-guided |" in streaming
    assert "| Row-level `foreach` sinks | design-gated |" in streaming


def _v9_entries() -> list[dict[str, Any]]:
    return _load(V9_LEDGER)["entries"]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _section(text: str, start: str, end: str) -> str:
    return text.split(start, 1)[1].split(end, 1)[0]
