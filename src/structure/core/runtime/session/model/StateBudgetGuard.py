from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, cast


class StateBudgetExceeded(RuntimeError):
    def __init__(self, *, step: str, operation: str, metric: str, limit: int, observed: int, batch_id: int) -> None:
        self.step = step
        self.operation = operation
        self.metric = metric
        self.limit = limit
        self.observed = observed
        self.batch_id = batch_id
        super().__init__(
            f"State budget exceeded in {step} ({operation}): {metric}={observed} exceeds {limit} "
            f"after batch {batch_id}. The query was stopped; inspect its checkpoint and Spark progress before restart."
        )


class StateBudgetGuard:
    """Observe a caller-owned query and stop it after a reported operator limit breach."""

    def __init__(self, *, runtime: object, policy: Mapping[str, object]) -> None:
        self._runtime = runtime
        self._policy = policy
        self._query: object | None = None
        self._listener = None
        self._error: StateBudgetExceeded | None = None

    @property
    def error(self) -> StateBudgetExceeded | None:
        return self._error

    def attach(self, query: object) -> "StateBudgetGuard":
        if self._query is not None:
            if self._query is query:
                return self
            raise ValueError("This state budget guard is already attached to a different query")
        operators = cast(Sequence[Mapping[str, object]], self._policy.get("operators", ()))
        if not operators or not any(
            item.get("max_rows") is not None or item.get("max_state_bytes") is not None for item in operators
        ):
            raise ValueError("The transform has no per-operator state limits for the guard to observe")
        if self._policy.get("track_rows") is False and any(
            item.get("max_rows") is not None for item in operators
        ):
            raise ValueError(
                "State row tracking is disabled by spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows; "
                "enable it before attaching max_rows limits."
            )
        streams = getattr(self._runtime, "streams", None)
        if streams is None:
            raise TypeError("State budget guard requires a SparkSession with a streams listener manager")
        self._query = query
        self._listener = self._build_listener()
        streams.addListener(self._listener)
        for progress in getattr(query, "recentProgress", ()):
            self._accept(progress)
        return self

    def check(self) -> None:
        if self._error is not None:
            raise self._error

    def close(self) -> None:
        if self._listener is not None:
            streams = getattr(self._runtime, "streams", None)
            if streams is not None:
                streams.removeListener(self._listener)
        self._listener = None
        self._query = None

    def _build_listener(self):
        from pyspark.sql.streaming import StreamingQueryListener

        guard = self

        class Listener(StreamingQueryListener):
            def onQueryStarted(self, event) -> None:
                return None

            def onQueryProgress(self, event) -> None:
                progress = event.progress
                query_id = getattr(progress, "id", getattr(progress, "queryId", None))
                target_id = getattr(guard._query, "id", None)
                if str(query_id) == str(target_id):
                    guard._accept(progress)

            def onQueryIdle(self, event) -> None:
                return None

            def onQueryTerminated(self, event) -> None:
                return None

        return Listener()

    def _accept(self, progress: object) -> None:
        if self._error is not None:
            return
        data = self._progress_dict(progress)
        state_operators = cast(
            Sequence[Mapping[str, object]], data.get("stateOperators", data.get("state_operators", ()))
        )
        expected = cast(Sequence[Mapping[str, object]], self._policy["operators"])
        if len(state_operators) != len(expected):
            self._fail_mapping(
                f"Spark reported {len(state_operators)} state operators for {len(expected)} declared state operations"
            )
            return
        mapped: dict[str, Mapping[str, object]] = {}
        for descriptor in expected:
            label = str(descriptor["operation"]).lower()
            tokens = ("dedupe", "dropduplicates") if "duplicate" in label else ("statestore", "aggregate")
            candidates = [
                item
                for item in state_operators
                if any(
                    token in str(item.get("operatorName", item.get("operator_name", ""))).lower()
                    for token in tokens
                )
            ]
            if len(candidates) != 1:
                self._fail_mapping("Spark progress operator names do not identify each declared operation uniquely")
                return
            mapped[str(descriptor["id"])] = candidates[0]
        batch_id = int(cast(Any, data.get("batchId", data.get("batch_id", -1))))
        for descriptor in expected:
            metrics = mapped[str(descriptor["id"])]
            for name, metric in (("numRowsTotal", "max_rows"), ("memoryUsedBytes", "max_state_bytes")):
                limit = descriptor.get(metric)
                observed = metrics.get(name)
                if limit is None or observed is None:
                    continue
                try:
                    observed_value = int(cast(Any, observed))
                except (TypeError, ValueError):
                    self._fail_mapping(f"Spark progress did not report numeric {name}")
                    return
                limit_value = int(cast(Any, limit))
                if observed_value > limit_value:
                    self._error = StateBudgetExceeded(
                        step=str(descriptor["step"]),
                        operation=str(descriptor["operation"]),
                        metric=metric,
                        limit=limit_value,
                        observed=observed_value,
                        batch_id=batch_id,
                    )
                    stop = getattr(self._query, "stop", None)
                    if callable(stop):
                        stop()
                    return

    @staticmethod
    def _progress_dict(progress: object) -> dict[str, object]:
        if isinstance(progress, Mapping):
            return dict(progress)
        as_dict = getattr(progress, "asDict", None)
        if callable(as_dict):
            return as_dict(recursive=True)
        json_value = getattr(progress, "json", None)
        if callable(json_value):
            return json.loads(json_value())
        if isinstance(json_value, str):
            return json.loads(json_value)
        if isinstance(progress, str):
            return json.loads(progress)
        raise TypeError("Unsupported Spark streaming progress object")

    @staticmethod
    def _fail_mapping(reason: str) -> None:
        raise RuntimeError(
            f"Cannot safely map Spark state progress to Structure state budgets: {reason}. "
            "The guard failed closed; inspect the Spark query plan and progress report."
        )
