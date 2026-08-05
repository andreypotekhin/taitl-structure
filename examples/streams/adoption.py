"""Caller-owned PySpark Structured Streaming recipes for the streams example."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from structure import Schema

RetryPolicy = Literal["at_least_once", "idempotent", "transactional"]
StateOperation = Literal["applyInPandasWithState", "transformWithState"]
StateTimeoutPolicy = Literal["none", "event_time", "processing_time"]
StateHookBoundary = Literal["caller-owned", "future-structure-hook"]
StateRestartPolicy = Literal["same_checkpoint", "new_checkpoint_on_schema_change"]


@dataclass(frozen=True)
class ForeachBatchSafety:
    """Caller-owned declarations needed before attaching an external batch sink.

    These values make the recovery assumptions reviewable; they do not make an
    arbitrary callback idempotent or transactional.
    """

    sink_identity: str
    idempotence_key: str
    retry_policy: RetryPolicy
    snapshot_id: str

    def validate(self) -> None:
        """Reject incomplete safety declarations before a query is started."""

        for name, value in (
            ("sink_identity", self.sink_identity),
            ("idempotence_key", self.idempotence_key),
            ("snapshot_id", self.snapshot_id),
        ):
            if not value.strip():
                raise ValueError(
                    f"FOREACH-BATCH-E0901: {name} must be a non-empty stable declaration; "
                    "see docs/api/Streaming.api.md#caller-owned-side-effect-safety"
                )
        if self.retry_policy not in {"at_least_once", "idempotent", "transactional"}:
            raise ValueError(
                f"FOREACH-BATCH-E0902: unsupported retry_policy {self.retry_policy!r}; "
                "use at_least_once, idempotent, or transactional; "
                "see docs/api/Streaming.api.md#caller-owned-side-effect-safety"
            )


@dataclass(frozen=True)
class ArbitraryStateContract:
    """Metadata required before caller-owned arbitrary state can be reviewed.

    This is a completeness guard, not an implementation of a PySpark state
    processor. The corresponding APIs remain design-gated until the contract
    has a Structure-owned runtime and live restart evidence.
    """

    operation: StateOperation
    input_schema: type[Schema]
    key_schema: type[Schema]
    state_schema: type[Schema]
    output_schema: type[Schema]
    grouping_key: tuple[str, ...]
    timeout_policy: StateTimeoutPolicy
    timeout_clock: Literal["event_time", "processing_time"] | None
    timeout_duration: str | None
    initialization_policy: str
    update_policy: str
    removal_policy: str
    target_profile: str
    hook_boundary: StateHookBoundary
    checkpoint_identity: str
    state_version: str
    restart_policy: StateRestartPolicy

    def validate(self) -> None:
        """Reject an incomplete state contract before caller code is adopted."""

        schemas = (
            ("input_schema", self.input_schema),
            ("key_schema", self.key_schema),
            ("state_schema", self.state_schema),
            ("output_schema", self.output_schema),
        )
        for name, schema in schemas:
            if not isinstance(schema, type) or not issubclass(schema, Schema):
                raise TypeError(
                    f"ARBITRARY-STATE-E0901: {name} must be a Structure Schema class; "
                    "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
                )

        if not self.grouping_key or any(not field.strip() for field in self.grouping_key):
            raise ValueError(
                "ARBITRARY-STATE-E0901: grouping_key must name at least one non-empty field; "
                "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
            )

        declarations = (
            ("initialization_policy", self.initialization_policy),
            ("update_policy", self.update_policy),
            ("removal_policy", self.removal_policy),
            ("target_profile", self.target_profile),
            ("checkpoint_identity", self.checkpoint_identity),
            ("state_version", self.state_version),
        )
        for name, value in declarations:
            if not value.strip():
                raise ValueError(
                    f"ARBITRARY-STATE-E0901: {name} must be a non-empty stable declaration; "
                    "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
                )

        if self.timeout_policy == "none":
            if self.timeout_clock is not None or self.timeout_duration is not None:
                raise ValueError(
                    "ARBITRARY-STATE-E0902: timeout_policy='none' cannot declare a clock or duration; "
                    "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
                )
        elif (
            self.timeout_clock != self.timeout_policy or not self.timeout_duration or not self.timeout_duration.strip()
        ):
            raise ValueError(
                "ARBITRARY-STATE-E0902: timed state must declare a matching timeout clock and duration; "
                "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
            )

        if self.operation not in {"applyInPandasWithState", "transformWithState"}:
            raise ValueError(
                f"ARBITRARY-STATE-E0903: unsupported operation {self.operation!r}; "
                "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
            )
        if self.hook_boundary not in {"caller-owned", "future-structure-hook"}:
            raise ValueError(
                f"ARBITRARY-STATE-E0903: unsupported hook_boundary {self.hook_boundary!r}; "
                "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
            )
        if self.restart_policy not in {"same_checkpoint", "new_checkpoint_on_schema_change"}:
            raise ValueError(
                f"ARBITRARY-STATE-E0903: unsupported restart_policy {self.restart_policy!r}; "
                "see docs/api/Streaming.api.md#typed-arbitrary-state-contract"
            )


def read_json_stream(spark: Any, schema: Any, source: Path | str) -> Any:
    """Create a JSON streaming source outside Structure transform execution."""

    return spark.readStream.schema(schema).json(str(source))


def start_memory_query(frame: Any, *, query_name: str, checkpoint: Path | str, output_mode: str) -> Any:
    """Start a test or demo memory sink for a transformed streaming DataFrame."""

    return (
        frame.writeStream.format("memory")
        .queryName(query_name)
        .outputMode(output_mode)
        .option("checkpointLocation", str(checkpoint))
        .start()
    )


def start_foreach_batch_query(
    frame: Any,
    callback: Any,
    *,
    checkpoint: Path | str,
    output_mode: str,
    safety: ForeachBatchSafety,
    trigger: dict[str, object] | None = None,
) -> Any:
    """Start a caller-owned foreachBatch sink for a transformed streaming DataFrame."""

    safety.validate()
    writer = (
        frame.writeStream.foreachBatch(callback).outputMode(output_mode).option("checkpointLocation", str(checkpoint))
    )
    if trigger is not None:
        writer = writer.trigger(**trigger)
    return writer.start()


def collect_available_memory_rows(query: Any, table: Any, *, order_by: str) -> list[dict[str, object]]:
    """Process currently available input and read deterministic rows from a memory sink."""

    query.processAllAvailable()
    return [row.asDict(recursive=True) for row in table.orderBy(order_by).collect()]


def stop_query(query: Any) -> None:
    """Stop a caller-owned streaming query."""

    query.stop()
