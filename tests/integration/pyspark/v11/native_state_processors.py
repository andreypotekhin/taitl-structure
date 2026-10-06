"""Importable native PySpark processors used by V11 runtime evidence."""

from pyspark.sql import Row
from pyspark.sql.streaming.stateful_processor import StatefulProcessor
from pyspark.sql.types import LongType, StringType, StructField, StructType


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


class NativePandasCompositeTotals(StatefulProcessor):
    """Exercise composite keys, Pandas initial state, native state kinds, TTL, and timers."""

    def init(self, handle) -> None:
        self._handle = handle
        self._total = handle.getValueState(
            "native_pandas_total",
            StructType([StructField("total", LongType(), nullable=False)]),
            ttlDurationMs=60000,
        )
        self._amounts = handle.getListState(
            "native_pandas_amounts",
            StructType([StructField("amount", LongType(), nullable=False)]),
            ttlDurationMs=60000,
        )
        self._items = handle.getMapState(
            "native_pandas_items",
            StructType([StructField("item_id", StringType(), nullable=False)]),
            StructType([StructField("amount", LongType(), nullable=False)]),
            ttlDurationMs=60000,
        )

    def handleInitialState(self, key, initialState, timerValues) -> None:
        total = int(initialState["seed_total"].sum())
        self._total.update((total,))
        self._amounts.appendValue((total,))

    def handleInputRows(self, key, rows, timerValues):
        import pandas as pd  # type: ignore[import-untyped]

        total = self._total.get()[0] if self._total.exists() else 0
        for batch in rows:
            for row in batch.itertuples(index=False):
                amount = int(row.amount)
                total += amount
                self._amounts.appendValue((amount,))
                existing = self._items.getValue((row.item_id,))
                previous = 0 if existing is None else int(existing[0])
                self._items.updateValue((row.item_id,), (previous + amount,))
        self._total.update((total,))
        expiry = timerValues.getCurrentProcessingTimeInMs() + 100
        self._handle.registerTimer(expiry)
        item_total = sum(int(value[0]) for value in self._items.values())
        return iter(
            [
                pd.DataFrame(
                    {
                        "customer_id": [key[0]],
                        "region": [key[1]],
                        "total": [total],
                        "amount_count": [sum(1 for _ in self._amounts.get())],
                        "item_total": [item_total],
                        "reason": ["input"],
                    }
                )
            ]
        )

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        import pandas as pd  # type: ignore[import-untyped]

        return iter(
            [
                pd.DataFrame(
                    {
                        "customer_id": [key[0]],
                        "region": [key[1]],
                        "total": [self._total.get()[0]],
                        "amount_count": [sum(1 for _ in self._amounts.get())],
                        "item_total": [sum(int(value[0]) for value in self._items.values())],
                        "reason": ["timer"],
                    }
                )
            ]
        )

    def close(self) -> None:
        pass


class NativePandasEvolutionV1(StatefulProcessor):
    """Write the original value schema and a state variable removed in the next processor version."""

    def init(self, handle) -> None:
        self._total = handle.getValueState(
            "evolution_total",
            StructType([StructField("total", LongType(), nullable=True)]),
        )
        self._removed = handle.getValueState(
            "evolution_removed",
            StructType([StructField("old_value", LongType(), nullable=True)]),
        )

    def handleInputRows(self, key, rows, timerValues):
        import pandas as pd  # type: ignore[import-untyped]

        current = self._total.get()
        total = 0 if current is None else int(current[0])
        for batch in rows:
            total += int(batch["amount"].sum())
        self._total.update((total,))
        self._removed.update((1,))
        return iter([pd.DataFrame({"customer_id": [key[0]], "total": [total], "marker": ["v1"]})])


class NativePandasEvolutionV2(StatefulProcessor):
    """Add/remove named state and widen the Avro value schema across a checkpoint restart."""

    def init(self, handle) -> None:
        handle.deleteIfExists("evolution_removed")
        self._total = handle.getValueState(
            "evolution_total",
            StructType(
                [
                    StructField("total", LongType(), nullable=True),
                    StructField("marker", StringType(), nullable=True),
                ]
            ),
        )
        self._added = handle.getValueState(
            "evolution_added",
            StructType([StructField("value", StringType(), nullable=True)]),
        )

    def handleInputRows(self, key, rows, timerValues):
        import pandas as pd  # type: ignore[import-untyped]

        current = self._total.get()
        total = 0 if current is None else int(current[0])
        for batch in rows:
            total += int(batch["amount"].sum())
        self._total.update((total, "v2"))
        self._added.update(("created",))
        return iter([pd.DataFrame({"customer_id": [key[0]], "total": [total], "marker": ["v2"]})])
