import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
RESOURCES = ROOT / "src/structure/plugin/pyspark/resources"
BATCH_CATALOG = RESOURCES / "pyspark-transformation-coverage.json"
STREAMING_LEDGER = RESOURCES / "pyspark-structured-streaming-coverage.json"
SUPPORTED = {"streaming-supported", "streaming-partial"}
VALID_STATUSES = SUPPORTED | {"streaming-ineligible", "streaming-deferred"}


@dataclass(frozen=True)
class Measurement:
    batch_supported: int
    batch_catalog_size: int
    streaming_supported: int
    streaming_supported_batch_families: int
    deferred_batch_families: list[str]
    ineligible_batch_families: list[str]

    @property
    def batch_ratio(self) -> str:
        return self._percent(self.batch_supported, self.batch_catalog_size)

    @property
    def streaming_ratio(self) -> str:
        return self._percent(self.streaming_supported, self.batch_catalog_size)

    @property
    def streaming_batch_family_ratio(self) -> str:
        return self._percent(self.streaming_supported_batch_families, self.batch_supported)

    @property
    def effective_streaming_denominator(self) -> int:
        return self.batch_supported - len(self.ineligible_batch_families)

    @property
    def effective_streaming_ratio(self) -> str:
        return self._percent(self.streaming_supported_batch_families, self.effective_streaming_denominator)

    def _percent(self, numerator: int, denominator: int) -> str:
        return f"{numerator / denominator:.1%}"


def test_structured_streaming_ledger_classifies_every_batch_supported_family() -> None:
    batch = _supported_batch_entries()
    ledger = _ledger_entries()

    batch_ids = [entry["id"] for entry in batch]
    ledger_ids = [entry["id"] for entry in ledger]

    assert len(ledger_ids) == len(set(ledger_ids))
    assert set(ledger_ids) == set(batch_ids)


def test_structured_streaming_ledger_entries_are_actionable() -> None:
    for entry in _ledger_entries():
        assert entry["status"] in VALID_STATUSES
        assert entry["state_class"]
        assert entry["output_mode"]
        assert entry["evidence"]
        assert entry["notes"]
        assert "readStream" not in entry["notes"]
        assert "writeStream" not in entry["notes"]
        assert entry["supported_operations"] or entry["rejected_operations"]
        for evidence in entry["evidence"]:
            assert (ROOT / evidence).is_file(), f"{entry['id']} evidence is missing: {evidence}"


def test_current_structured_streaming_measurement_is_checked() -> None:
    measurement = _measure()

    assert measurement.batch_supported == 40
    assert measurement.batch_catalog_size == 43
    assert measurement.streaming_supported == 38
    assert measurement.streaming_supported_batch_families == 38
    assert measurement.deferred_batch_families == []
    assert measurement.ineligible_batch_families == [
        "dataframe.ordering",
        "dataframe.priority-selection",
    ]
    assert measurement.batch_ratio == "93.0%"
    assert measurement.streaming_ratio == "88.4%"
    assert measurement.streaming_batch_family_ratio == "95.0%"
    assert measurement.effective_streaming_denominator == 38
    assert measurement.effective_streaming_ratio == "100.0%"


def test_target_gated_variant_streaming_family_has_live_profile_evidence() -> None:
    entry = {entry["id"]: entry for entry in _ledger_entries()}["functions.variant"]

    assert entry["status"] == "streaming-supported"
    assert "tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py" in entry["evidence"]
    assert "tests/integration/pyspark/v3/streams/test_file_streams.py" in entry["evidence"]
    assert "PySpark 4.0 streaming evidence" in entry["notes"]
    assert "PySpark 3.5 evidence proves target rejection" in entry["notes"]


def _measure() -> Measurement:
    batch_catalog = _load(BATCH_CATALOG)["entries"]
    ledger = _ledger_entries()
    supported_entries = [entry for entry in batch_catalog if entry["status"] == "supported"]
    supported_ledger = [entry for entry in ledger if entry["status"] in SUPPORTED]

    return Measurement(
        batch_supported=len(supported_entries),
        batch_catalog_size=len(batch_catalog),
        streaming_supported=len(supported_ledger),
        streaming_supported_batch_families=len(supported_ledger),
        deferred_batch_families=[entry["id"] for entry in ledger if entry["status"] == "streaming-deferred"],
        ineligible_batch_families=[entry["id"] for entry in ledger if entry["status"] == "streaming-ineligible"],
    )


def _supported_batch_entries() -> list[dict[str, Any]]:
    return [entry for entry in _load(BATCH_CATALOG)["entries"] if entry["status"] == "supported"]


def _ledger_entries() -> list[dict[str, Any]]:
    return _load(STREAMING_LEDGER)["entries"]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
