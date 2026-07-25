from collections.abc import Callable, Iterator
from typing import Any

import pytest
from integration.pyspark.support.backend_matrix import spark
from integration.pyspark.support.rows import clear_rows


@pytest.fixture(autouse=True)
def materialized_rows() -> Iterator[None]:
    clear_rows()
    yield
    clear_rows()


@pytest.fixture
def cache_frames() -> Iterator[Callable[..., None]]:
    cached: list[Any] = []

    def cache(*frames: Any) -> None:
        cached.extend(frame.persist() for frame in frames)

    yield cache

    for frame in reversed(cached):
        frame.unpersist()


__all__ = ["cache_frames", "spark"]
