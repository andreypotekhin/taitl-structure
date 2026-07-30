"""Swappable, deterministic offline rankers for the Search example.

The module deliberately has no Spark dependency.  Callers persist a bounded
``DocumentTrainingData`` snapshot, translate it to ``TrainingExample`` values,
and then run these rankers on the driver.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from math import exp, isfinite, sqrt
from typing import Mapping, Protocol, Sequence

FEATURE_CONTRACT_VERSION = "search-document-v1"


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
class RankingMetrics:
    """Offline validation metrics for one ranker or the lexical baseline."""

    ndcg_at_5: float
    ndcg_at_10: float
    mean_reciprocal_rank: float
    query_count: int
    covered_query_count: int


@dataclass(frozen=True)
class RankingArtifact:
    """Immutable, portable description of a selected ranking model."""

    model_id: str
    ranker_id: str
    artifact_version: int
    feature_contract_version: str
    feature_names: tuple[str, ...]
    intercept: float
    means: Mapping[str, float]
    scales: Mapping[str, float]
    weights: Mapping[str, float]
    snapshot_id: str
    split_seed: str
    validation: RankingMetrics
    metadata: Mapping[str, str]

    def __post_init__(self) -> None:
        if not self.model_id or not self.ranker_id or not self.snapshot_id:
            raise ValueError("Ranking artifacts require nonblank model_id, ranker_id, and snapshot_id.")
        if self.artifact_version <= 0 or not self.feature_contract_version:
            raise ValueError("Ranking artifacts require a positive version and feature contract version.")
        if not self.feature_names or tuple(sorted(set(self.feature_names))) != self.feature_names:
            raise ValueError("Ranking-artifact feature_names must be a nonempty sorted unique tuple.")
        if not isfinite(self.intercept):
            raise ValueError("Ranking-artifact intercept must be finite.")
        for name in self.feature_names:
            values = (self.means.get(name), self.scales.get(name), self.weights.get(name))
            if any(value is None or not isfinite(value) for value in values) or self.scales[name] <= 0.0:
                raise ValueError(f"Ranking-artifact feature '{name}' requires finite mean, positive scale, and weight.")


class RankerScorer(Protocol):
    """Scores one feature mapping from a validated ranking artifact."""

    def score(self, features: Mapping[str, float]) -> float: ...


class RankerTrainer(Protocol):
    """Fits one versioned ranker and creates a portable artifact."""

    ranker_id: str
    artifact_version: int

    def fit(
        self, rows: Sequence[TrainingExample], *, snapshot_id: str, split_seed: str, validation: RankingMetrics
    ) -> RankingArtifact: ...


class RankerCatalog:
    """Maps stable artifact identities to interchangeable trainer implementations."""

    def __init__(self, trainers: Sequence[RankerTrainer] = ()) -> None:
        self._trainers: dict[tuple[str, int], RankerTrainer] = {}
        for trainer in trainers:
            self.register(trainer)

    def register(self, trainer: RankerTrainer) -> None:
        key = (trainer.ranker_id, trainer.artifact_version)
        if not trainer.ranker_id or trainer.artifact_version <= 0:
            raise ValueError("Rankers require a nonblank ID and positive artifact version.")
        if key in self._trainers:
            raise ValueError(f"Ranker '{trainer.ranker_id}' version {trainer.artifact_version} is already registered.")
        self._trainers[key] = trainer

    def trainer(self, ranker_id: str, artifact_version: int) -> RankerTrainer:
        try:
            return self._trainers[(ranker_id, artifact_version)]
        except KeyError as error:
            raise ValueError(
                f"No ranker is registered for '{ranker_id}' artifact version {artifact_version}."
            ) from error

    @property
    def trainers(self) -> tuple[RankerTrainer, ...]:
        """Registered trainers in deterministic identity order."""
        return tuple(self._trainers[key] for key in sorted(self._trainers))

    def scorer(self, artifact: RankingArtifact) -> RankerScorer:
        trainer = self.trainer(artifact.ranker_id, artifact.artifact_version)
        if artifact.feature_contract_version != FEATURE_CONTRACT_VERSION:
            raise ValueError(
                f"Ranker '{trainer.ranker_id}' cannot serve feature contract '{artifact.feature_contract_version}'."
            )
        return LinearArtifactScorer(artifact)


@dataclass(frozen=True)
class LinearArtifactScorer:
    """Common serving scorer for standardized linear artifacts."""

    artifact: RankingArtifact

    def score(self, features: Mapping[str, float]) -> float:
        missing = [name for name in self.artifact.feature_names if name not in features]
        if missing:
            raise ValueError(f"Ranking artifact '{self.artifact.model_id}' requires feature(s): {', '.join(missing)}.")
        return self.artifact.intercept + sum(
            self.artifact.weights[name] * (features[name] - self.artifact.means[name]) / self.artifact.scales[name]
            for name in self.artifact.feature_names
        )


@dataclass(frozen=True)
class LinearTrainingSettings:
    """Shared deterministic batch-gradient settings for built-in linear rankers."""

    epochs: int = 200
    learning_rate: float = 0.05
    l2: float = 0.01

    def __post_init__(self) -> None:
        if self.epochs <= 0 or self.learning_rate <= 0.0 or self.l2 < 0.0:
            raise ValueError("Linear ranker settings require positive epochs/rate and nonnegative L2.")


class _LinearTrainer:
    artifact_version = 1
    ranker_id: str
    objective: str

    def __init__(self, settings: LinearTrainingSettings = LinearTrainingSettings()) -> None:
        self.settings = settings

    def fit(
        self, rows: Sequence[TrainingExample], *, snapshot_id: str, split_seed: str, validation: RankingMetrics
    ) -> RankingArtifact:
        if not rows:
            raise ValueError(f"{self.ranker_id} requires at least one training example.")
        rows = tuple(sorted(rows, key=lambda row: (row.query_id, row.document_id)))
        names, means, scales = _statistics(rows)
        intercept, weights = self._fit(rows, names, means, scales)
        return RankingArtifact(
            model_id=_model_id(self.ranker_id, snapshot_id, split_seed, names, weights),
            ranker_id=self.ranker_id,
            artifact_version=self.artifact_version,
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            feature_names=names,
            intercept=intercept,
            means=means,
            scales=scales,
            weights=weights,
            snapshot_id=snapshot_id,
            split_seed=split_seed,
            validation=validation,
            metadata={"objective": self.objective},
        )

    def _fit(self, rows, names, means, scales):
        raise NotImplementedError


class GradeRegressionRanker(_LinearTrainer):
    """Least-squares relevance-grade predictor."""

    ranker_id = "grade-regression"
    objective = "least-squares relevance grade"

    def _fit(self, rows, names, means, scales):
        intercept, weights = 0.0, {name: 0.0 for name in names}
        for _ in range(self.settings.epochs):
            bias_gradient, gradients = 0.0, {name: 0.0 for name in names}
            for row in rows:
                values = _standardized(row, names, means, scales)
                error = intercept + sum(weights[name] * values[name] for name in names) - row.relevance
                bias_gradient += error
                for name in names:
                    gradients[name] += error * values[name]
            count = float(len(rows))
            intercept -= self.settings.learning_rate * bias_gradient / count
            for name in names:
                weights[name] -= self.settings.learning_rate * (
                    gradients[name] / count + self.settings.l2 * weights[name]
                )
        return intercept, weights


class PairwiseLinearRanker(_LinearTrainer):
    """Logistic ranker over every unequal-grade pair within a query."""

    ranker_id = "pairwise-linear"
    objective = "pairwise logistic ranking"

    def _fit(self, rows, names, means, scales):
        pairs = tuple(_pairs(rows, names, means, scales))
        if not pairs:
            raise ValueError("Pairwise linear ranking requires one query with unequal relevance grades.")
        weights = {name: 0.0 for name in names}
        for _ in range(self.settings.epochs):
            gradients = {name: 0.0 for name in names}
            for difference in pairs:
                margin = sum(weights[name] * difference[name] for name in names)
                factor = -1.0 / (1.0 + exp(min(max(margin, -700.0), 700.0)))
                for name in names:
                    gradients[name] += factor * difference[name]
            count = float(len(pairs))
            for name in names:
                weights[name] -= self.settings.learning_rate * (
                    gradients[name] / count + self.settings.l2 * weights[name]
                )
        return 0.0, weights


def _statistics(rows: Sequence[TrainingExample]):
    names = tuple(sorted({name for row in rows for name in row.features}))
    means = {name: sum(row.features.get(name, 0.0) for row in rows) / len(rows) for name in names}
    scales = {
        name: max(sqrt(sum((row.features.get(name, 0.0) - means[name]) ** 2 for row in rows) / len(rows)), 1.0)
        for name in names
    }
    return names, means, scales


def _standardized(row, names, means, scales):
    return {name: (row.features.get(name, 0.0) - means[name]) / scales[name] for name in names}


def _pairs(rows, names, means, scales):
    by_query: dict[str, list[TrainingExample]] = {}
    for row in rows:
        by_query.setdefault(row.query_id, []).append(row)
    for query_rows in by_query.values():
        ordered = sorted(query_rows, key=lambda row: (row.document_id, row.relevance))
        for left_index, left in enumerate(ordered):
            for right in ordered[left_index + 1 :]:
                if left.relevance == right.relevance:
                    continue
                winner, loser = (left, right) if left.relevance > right.relevance else (right, left)
                high, low = _standardized(winner, names, means, scales), _standardized(loser, names, means, scales)
                yield {name: high[name] - low[name] for name in names}


def _model_id(ranker_id, snapshot_id, split_seed, names, weights):
    payload = "|".join((ranker_id, snapshot_id, split_seed, *names, *(repr(weights[name]) for name in names)))
    return f"{ranker_id}-{sha256(payload.encode()).hexdigest()[:16]}"
