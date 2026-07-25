from integration.pyspark.support.rows import clear_rows, rows, single


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
