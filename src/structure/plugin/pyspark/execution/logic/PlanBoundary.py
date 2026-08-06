from __future__ import annotations

from uuid import uuid4


class PlanBoundaryTracker:

    def __init__(self, spark) -> None:
        self._spark = spark
        self._views: set[str] = set()

    def apply(self, frame):
        name = f"_structure_boundary_{uuid4().hex}"
        frame.createOrReplaceTempView(name)
        self._views.add(name)
        return self._spark.table(name)

    def close(self) -> None:
        for name in tuple(self._views):
            try:
                self._spark.catalog.dropTempView(name)
            except Exception:
                pass
            finally:
                self._views.discard(name)


_TRACKERS: dict[int, PlanBoundaryTracker] = {}


def _tracker(spark) -> PlanBoundaryTracker:
    key = id(spark)
    tracker = _TRACKERS.get(key)
    if tracker is None or tracker._spark is not spark:
        tracker = PlanBoundaryTracker(spark)
        _TRACKERS[key] = tracker
    return tracker


def apply_plan_boundary(frame, spark):
    return _tracker(spark).apply(frame)


def close_plan_boundaries(spark) -> None:
    tracker = _TRACKERS.pop(id(spark), None)
    if tracker is not None:
        tracker.close()
