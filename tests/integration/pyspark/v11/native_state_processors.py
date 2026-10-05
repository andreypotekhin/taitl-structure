"""Importable native PySpark processors used by V11 runtime evidence."""

from pyspark.sql import Row
from pyspark.sql.streaming.stateful_processor import StatefulProcessor
from pyspark.sql.types import LongType, StructField, StructType


class NativeInitialTotals(StatefulProcessor):
    """Seed two native value states, then add input rows for each key."""

    def init(self, handle) -> None:
        self._handle = handle
        self._total = handle.getValueState(
            "native_total",
            StructType([StructField("total", LongType(), nullable=False)]),
        )
        self._count = handle.getValueState(
            "native_count",
            StructType([StructField("count", LongType(), nullable=False)]),
        )

    def handleInitialState(self, key, initialState, timerValues) -> None:
        self._total.update((initialState["total"],))
        self._count.update((initialState["count"],))

    def handleInputRows(self, key, rows, timerValues):
        total = self._total.get()[0] if self._total.exists() else 0
        count = self._count.get()[0] if self._count.exists() else 0
        for row in rows:
            total += row["amount"]
            count += 1
        self._total.update((total,))
        self._count.update((count,))
        self._handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 100)
        return iter([Row(customer_id=key[0], total=total, count=count, reason="input")])

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        return iter(
            [
                Row(
                    customer_id=key[0],
                    total=self._total.get()[0],
                    count=self._count.get()[0],
                    reason="timer",
                )
            ]
        )
