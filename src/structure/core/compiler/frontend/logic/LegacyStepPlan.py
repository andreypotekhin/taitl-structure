from dataclasses import dataclass
from typing import Any

from structure.plugin.api.v1.model.StepPlan import StepPlan
from structure.plugin.api.v1.model.StepResultPlan import StepResultPlan


@dataclass(frozen=True)
class LegacyStepResultPlan(StepResultPlan):
    """Temporary PySpark payload retained only by the transitional Core authorer."""

    projection: tuple[Any, ...] = ()
    aggregate: Any = None


@dataclass(frozen=True)
class LegacyStepPlan(StepPlan):
    """Temporary Core compatibility record for the retired PySpark compiler path."""

    filters: tuple[Any, ...] = ()
    projection: tuple[Any, ...] = ()
    aggregate: Any = None
    joins: tuple[Any, ...] = ()
    operations: tuple[Any, ...] = ()
