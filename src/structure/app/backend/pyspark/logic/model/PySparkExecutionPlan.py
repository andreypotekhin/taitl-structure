from __future__ import annotations

from dataclasses import dataclass

from structure.app.backend.capabilities.logic.model.BackendId import BackendId
from structure.app.backend.pyspark.logic.model.PySparkInputRecipe import PySparkInputRecipe
from structure.app.backend.pyspark.logic.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.backend.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkExecutionPlan:
    transform: str
    backend: BackendId
    inputs: tuple[PySparkInputRecipe, ...]
    steps: tuple[PySparkStepRecipe, ...]
    final_validation: PySparkValidationRecipe
    requires_hook_inputs: bool
