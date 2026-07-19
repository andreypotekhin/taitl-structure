from __future__ import annotations

from dataclasses import dataclass

from structure.core.target.capabilities.model.BackendId import BackendId
from structure.platform.pyspark.model.PySparkInputRecipe import PySparkInputRecipe
from structure.platform.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.platform.pyspark.model.PySparkStepRecipe import PySparkStepRecipe
from structure.platform.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkExecutionPlan:
    transform: str
    backend: BackendId
    inputs: tuple[PySparkInputRecipe, ...]
    steps: tuple[PySparkStepRecipe, ...]
    outputs: tuple[PySparkOutputRecipe, ...]
    requires_hook_inputs: bool

    @property
    def final_validation(self) -> PySparkValidationRecipe:
        if len(self.outputs) != 1:
            names = ", ".join(output.name for output in self.outputs)
            raise ValueError(f"Transform has multiple outputs: {names}. Use PySparkExecutionPlan.outputs instead.")
        return self.outputs[0].validation
