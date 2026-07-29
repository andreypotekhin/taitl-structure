"""A deterministic, dependency-free offline linear-ranking baseline."""

from dataclasses import dataclass
from math import isfinite
from typing import Mapping, Sequence


@dataclass(frozen=True)
class TrainingExample:
    """One caller-labeled query/document feature row for offline training."""

    query_id: str
    document_id: str
    relevance: float
    features: Mapping[str, float]

    def __post_init__(self) -> None:
        if not self.query_id or not self.document_id:
            raise ValueError("Training examples require nonblank query_id and document_id.")
        if not isfinite(self.relevance):
            raise ValueError("Training-example relevance must be finite.")
        invalid = next((name for name, value in self.features.items() if not name or not isfinite(value)), None)
        if invalid is not None:
            raise ValueError(f"Training-example feature '{invalid}' must have a nonblank name and finite value.")


@dataclass(frozen=True)
class LinearRanker:
    """A portable linear scorer whose caller owns persistence and serving."""

    bias: float
    weights: Mapping[str, float]

    def score(self, features: Mapping[str, float]) -> float:
        return self.bias + sum(self.weights.get(name, 0.0) * value for name, value in features.items())

    def rank(self, query_id: str, examples: Sequence[TrainingExample]) -> list[TrainingExample]:
        return sorted(
            (example for example in examples if example.query_id == query_id),
            key=lambda example: (-self.score(example.features), example.document_id),
        )


@dataclass(frozen=True)
class LinearTraining:
    """Deterministic batch-gradient training settings for :class:`LinearRanker`."""

    epochs: int = 200
    learning_rate: float = 0.05
    l2: float = 0.01

    def __post_init__(self) -> None:
        if self.epochs <= 0:
            raise ValueError("Linear-training epochs must be positive.")
        if self.learning_rate <= 0.0:
            raise ValueError("Linear-training learning_rate must be positive.")
        if self.l2 < 0.0:
            raise ValueError("Linear-training l2 must be nonnegative.")

    def train(self, examples: Sequence[TrainingExample]) -> LinearRanker:
        """Fit a least-squares baseline without nondeterministic sampling or shuffling."""
        if not examples:
            raise ValueError("Linear training requires at least one labeled example.")
        ordered = sorted(examples, key=lambda example: (example.query_id, example.document_id))
        names = tuple(sorted({name for example in ordered for name in example.features}))
        weights = {name: 0.0 for name in names}
        bias = 0.0
        count = float(len(ordered))
        for _ in range(self.epochs):
            bias_gradient = 0.0
            gradients = {name: 0.0 for name in names}
            for example in ordered:
                error = bias + sum(weights[name] * example.features.get(name, 0.0) for name in names) - example.relevance
                bias_gradient += error
                for name in names:
                    gradients[name] += error * example.features.get(name, 0.0)
            bias -= self.learning_rate * bias_gradient / count
            for name in names:
                weights[name] -= self.learning_rate * (gradients[name] / count + self.l2 * weights[name])
        return LinearRanker(bias=bias, weights=weights)
