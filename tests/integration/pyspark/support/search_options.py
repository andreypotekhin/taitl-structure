from __future__ import annotations

import os

_STAGE_OUTPUTS_VARIABLE = "STRUCTURE_SEARCH_STAGE_OUTPUTS"


def stage_outputs_enabled() -> bool:
    """Resolve the Search integration stage-output comparison switch.

    The default is final-results-only.  Setting the variable to ``1`` enables
    the same stage packaging that the library supports by default, which lets
    a benchmark compare the two paths without changing production defaults.
    """

    value = os.environ.get(_STAGE_OUTPUTS_VARIABLE, "0")
    if value not in {"0", "1"}:
        raise ValueError(f"{_STAGE_OUTPUTS_VARIABLE} must be exactly 0 or 1; got {value!r}")
    return value == "1"
