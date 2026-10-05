from __future__ import annotations

from uuid import uuid4


class PlanBoundaryTracker:

    def __init__(self, spark) -> None:
        self._spark = spark
        self._views: dict[int, tuple[object | None, set[str]]] = {}

    def apply(self, frame, owner=None):
        name = f"_structure_boundary_{uuid4().hex}"
        frame.createOrReplaceTempView(name)
        key = 0 if owner is None else id(owner)
        entry = self._views.setdefault(key, (owner, set()))
        entry[1].add(name)
        return self._spark.table(name)

    def close(self, owner=None) -> None:
        keys = tuple(self._views) if owner is None else (id(owner),)
        for key in keys:
            entry = self._views.pop(key, None)
            if entry is None:
                continue
            for name in tuple(entry[1]):
                self._drop(name)

    def _drop(self, name: str) -> None:
        try:
            self._spark.catalog.dropTempView(name)
        except Exception:
            pass


_TRACKERS: dict[int, PlanBoundaryTracker] = {}


def _tracker(spark) -> PlanBoundaryTracker:
    key = id(spark)
    tracker = _TRACKERS.get(key)
    if tracker is None or tracker._spark is not spark:
        tracker = PlanBoundaryTracker(spark)
        _TRACKERS[key] = tracker
    return tracker


def apply_plan_boundary(frame, spark, owner=None):
    return _tracker(spark).apply(frame, owner=owner)


def close_plan_boundaries(spark, owner=None) -> None:
    if owner is None:
        tracker = _TRACKERS.get(id(spark))
        if tracker is not None:
            tracker.close()
            _TRACKERS.pop(id(spark), None)
        return
    # foreachBatch may supply a DataFrame backed by a SparkSession wrapper
    # distinct from the session that originally compiled the streaming plan.
    # The owner is stable, so release its views from every session it used.
    for key, tracker in tuple(_TRACKERS.items()):
        tracker.close(owner=owner)
        if not tracker._views:
            _TRACKERS.pop(key, None)
