from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter


@contextmanager
def phase(label: str) -> Iterator[None]:
    started = perf_counter()
    print(f"[phase] {label}: starting", flush=True)
    try:
        yield
    finally:
        print(f"[phase] {label}: {perf_counter() - started:.2f}s", flush=True)
