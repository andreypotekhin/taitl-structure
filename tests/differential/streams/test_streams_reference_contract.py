from datetime import datetime

from helpers.example_projects import render_streams_example


def test_streams_reference_enrichment_removes_duplicate_timing_messages() -> None:
    passages = _prepare_passages(
        events=[
            _event("e-1", 1_000),
            _event("e-1", 1_000),
        ],
        races=[{"id": "r-1", "name": "River Run", "city": "Sierra", "country": "USA"}],
        paddlers=[{"race_id": "r-1", "id": "p-1", "name": "Ava Stone", "country": "NZL", "bib": 12}],
        gates=[{"race_id": "r-1", "number": 4, "direction": "upstream", "sector": "Narrows"}],
    )

    assert passages == [
        {
            "id": "e-1",
            "race_name": "River Run",
            "race_city": "Sierra",
            "race_country": "USA",
            "paddler_name": "Ava Stone",
            "paddler_country": "NZL",
            "bib": 12,
            "gate_direction": "upstream",
            "sector": "Narrows",
        }
    ]


def test_streams_generated_code_keeps_lifecycle_with_the_caller() -> None:
    generated = "\n".join(render_streams_example().values())

    for fragment in (
        'withWatermark("at", \'10 minutes\')',
        '.dropDuplicates(["id"])',
        'passages.groupBy(\n            F.col("passage.race_id").alias("race_id")',
        'F.expr("INTERVAL 5 minutes")',
    ):
        assert fragment in generated
    for lifecycle_api in ("readStream", "writeStream", ".start(", "awaitTermination"):
        assert lifecycle_api not in generated


def _prepare_passages(*, events, races, paddlers, gates):
    race_by_id = {race["id"]: race for race in races}
    paddler_by_key = {(paddler["race_id"], paddler["id"]): paddler for paddler in paddlers}
    gate_by_key = {(gate["race_id"], gate["number"]): gate for gate in gates}
    passages = []
    seen = set()
    for event in events:
        if event["id"] in seen or event["elapsed_millis"] < 0:
            continue
        seen.add(event["id"])
        race = race_by_id[event["race_id"]]
        paddler = paddler_by_key[(event["race_id"], event["paddler_id"])]
        gate = gate_by_key[(event["race_id"], event["gate_number"])]
        passages.append(
            {
                "id": event["id"],
                "race_name": race["name"],
                "race_city": race["city"],
                "race_country": race["country"],
                "paddler_name": paddler["name"],
                "paddler_country": paddler["country"],
                "bib": paddler["bib"],
                "gate_direction": gate["direction"],
                "sector": gate["sector"],
            }
        )
    return passages


def _event(id: str, elapsed_millis: int) -> dict[str, object]:
    return {
        "id": id,
        "race_id": "r-1",
        "run_id": "r-1-heat-1",
        "paddler_id": "p-1",
        "gate_number": 4,
        "at": datetime(2026, 7, 12, 10, 0),
        "elapsed_millis": elapsed_millis,
    }
