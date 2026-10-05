from __future__ import annotations

from types import SimpleNamespace

import pytest

from structure.core.runtime.session.model.StateBudgetGuard import StateBudgetExceeded, StateBudgetGuard


class Query:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


def _policy(*, max_rows: int | None = None, max_state_bytes: int | None = None):
    return {
        "operators": (
            {
                "id": "0:1",
                "step": "summarize",
                "operation": "drop_duplicates",
                "max_rows": max_rows,
                "max_state_bytes": max_state_bytes,
            },
            {
                "id": "0:2",
                "step": "summarize",
                "operation": "aggregate",
                "max_rows": None,
                "max_state_bytes": None,
            },
        ),
        "track_rows": True,
    }


def test_state_budget_guard_stops_query_and_exposes_typed_breach() -> None:
    query = Query()
    guard = StateBudgetGuard(runtime=SimpleNamespace(), policy=_policy(max_rows=2))
    guard._query = query

    guard._accept(
        {
            "batchId": 7,
            "stateOperators": [
                {"operatorName": "dedupe", "numRowsTotal": 3, "memoryUsedBytes": 100},
                {"operatorName": "stateStoreSave", "numRowsTotal": 1, "memoryUsedBytes": 200},
            ],
        }
    )

    assert query.stopped
    assert isinstance(guard.error, StateBudgetExceeded)
    assert guard.error.batch_id == 7
    assert guard.error.step == "summarize"
    assert guard.error.metric == "max_rows"
    assert guard.error.observed == 3
    with pytest.raises(StateBudgetExceeded, match="query was stopped"):
        guard.check()


def test_state_budget_guard_fails_closed_when_operator_mapping_is_ambiguous() -> None:
    guard = StateBudgetGuard(runtime=SimpleNamespace(), policy=_policy(max_rows=2))
    guard._query = Query()

    with pytest.raises(RuntimeError, match="failed closed"):
        guard._accept(
            {
                "batchId": 1,
                "stateOperators": [
                    {"operatorName": "stateStoreSave", "numRowsTotal": 1, "memoryUsedBytes": 100},
                    {"operatorName": "stateStoreSave", "numRowsTotal": 2, "memoryUsedBytes": 200},
                ],
            }
        )


def test_state_budget_guard_rejects_row_limits_when_row_tracking_is_disabled() -> None:
    guard = StateBudgetGuard(runtime=SimpleNamespace(), policy={**_policy(max_rows=2), "track_rows": False})

    with pytest.raises(ValueError, match="row tracking is disabled"):
        guard.attach(Query())
