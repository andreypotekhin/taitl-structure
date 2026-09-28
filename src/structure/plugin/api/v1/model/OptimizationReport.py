from dataclasses import dataclass


@dataclass(frozen=True)
class OptimizationDecision:
    step: str
    retained: bool
    reason: str


@dataclass(frozen=True)
class OptimizationReport:
    """An ordered whole-step selection and its explanation."""

    decisions: tuple[OptimizationDecision, ...]

    @property
    def retained_steps(self) -> tuple[str, ...]:
        return tuple(item.step for item in self.decisions if item.retained)

    @property
    def removed_steps(self) -> tuple[str, ...]:
        return tuple(item.step for item in self.decisions if not item.retained)

    def explain(self) -> str:
        lines = [f"Unused-step pruning: {len(self.decisions)} original, {len(self.retained_steps)} retained steps."]
        for item in self.decisions:
            action = "Kept" if item.retained else "Removed"
            lines.append(f"{action} {item.step}: {item.reason}.")
        return "\n".join(lines)
