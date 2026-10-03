"""Semantic settings visible while a plugin authors a symbolic transform body."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

_settings: ContextVar[Mapping[str, object]] = ContextVar("structure_compilation_settings", default={})


def current_compilation_settings() -> Mapping[str, object]:
    return _settings.get()


@contextmanager
def compilation_settings(values: Mapping[str, object]) -> Iterator[None]:
    token = _settings.set(values)
    try:
        yield
    finally:
        _settings.reset(token)
