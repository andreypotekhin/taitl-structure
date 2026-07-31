"""Caller-owned PySpark Structured Streaming recipes for the streams example."""
from pathlib import Path
from typing import Any


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
    trigger: dict[str, object] | None = None,
) -> Any:
    """Start a caller-owned foreachBatch sink for a transformed streaming DataFrame."""

    writer = (
        frame.writeStream.foreachBatch(callback)
        .outputMode(output_mode)
        .option("checkpointLocation", str(checkpoint))
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
