from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StreamingStateStage:
    """Compiler-visible state and retention metadata for one streaming stage."""

    step: str
    operation: str
    event_time: tuple[str, ...] = ()
    watermarks: tuple[tuple[str, str], ...] = ()
    keys: tuple[str, ...] = ()
    retention: tuple[str, ...] = ()
    order_keys: tuple[str, ...] = ()
    completion_window: str | None = None
    output_modes: tuple[str, ...] = ()
    allows_later_stateful: bool = False
