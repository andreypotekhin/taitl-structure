import pytest
from integration.pyspark.support.rows import clear_rows, rows, single


@pytest.fixture(autouse=True)
def row_cache():
    clear_rows()
    yield
    clear_rows()


class _Row:
    def __init__(self, value: int) -> None:
        self.value = value

    def asDict(self, *, recursive: bool) -> dict[str, int]:
        return {"value": self.value}


class _Frame:
    def __init__(self) -> None:
        self.collect_count = 0

    def collect(self) -> list[_Row]:
        self.collect_count += 1
        return [_Row(1)]

    def orderBy(self, *columns: str) -> "_Frame":
        return self


def test_rows_reuses_a_frame_materialization_within_one_test() -> None:
    frame = _Frame()

    assert rows(frame) == [{"value": 1}]
    assert single(frame, lambda row: row["value"] == 1) == {"value": 1}
    assert frame.collect_count == 1


def test_rows_cache_is_explicitly_clearable() -> None:
    frame = _Frame()

    rows(frame)
    clear_rows()
    rows(frame)

    assert frame.collect_count == 2


def test_ordered_rows_also_satisfy_unordered_assertions() -> None:
    frame = _Frame()

    result = rows(frame, "value")
    result[0]["value"] = 99

    assert single(frame, lambda row: row["value"] == 1) == {"value": 1}
    assert rows(frame, "value") == [{"value": 1}]
    assert frame.collect_count == 1


def test_ordering_and_recursive_requests_are_not_interchangeable() -> None:
    frame = _Frame()

    rows(frame)
    rows(frame, "value")
    rows(frame, "other")
    rows(frame, recursive=False)

    assert frame.collect_count == 4
