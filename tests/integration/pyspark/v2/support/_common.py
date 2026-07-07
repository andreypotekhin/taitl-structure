from __future__ import annotations

import importlib
from typing import Any


def transform_type(module: str, name: str) -> Any:
    return getattr(importlib.import_module(module), name)
