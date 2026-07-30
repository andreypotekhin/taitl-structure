"""Deterministic train, validate, and recommend orchestration for Search rankers."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from math import log2
from typing import Sequence

from examples.search.algorithms.training.rankers import (
    GradeRegressionRanker,
    LinearArtifactScorer,
    PairwiseLinearRanker,
    RankerCatalog,
    RankerTrainer,
    RankingArtifact,
    RankingMetrics,
    TrainingExample,
)


@dataclass(frozen=True)
class TrainingSplit:
    """A reproducible whole-query train/validation split."""

    seed: str = "search-v1"
    validation_percent: int = 20

    def __post_init__(self) -> None:
        if not self.seed or not 0 < self.validation_percent < 100:
            raise ValueError("Training split requires a nonblank seed and validation_percent between 1 and 99.")

    def partition(
        self, rows: Sequence[TrainingExample]
    ) -> tuple[tuple[TrainingExample, ...], tuple[TrainingExample, ...]]:
        train: list[TrainingExample] = []
        validation: list[TrainingExample] = []
        for row in sorted(rows, key=lambda value: (value.query_id, value.document_id)):
            target = validation if self._validation(row.query_id) else train
            target.append(row)
        if not train or not validation:
            raise ValueError(
                "Training split produced an empty train or validation partition; adjust the snapshot or seed."
            )
        return tuple(train), tuple(validation)

    def _validation(self, query_id: str) -> bool:
        digest = sha256(f"{self.seed}:{query_id}".encode()).digest()
        return int.from_bytes(digest[:8], "big") % 100 < self.validation_percent


@dataclass(frozen=True)
class TrainingCandidate:
    """One evaluated artifact candidate."""

    artifact: RankingArtifact
    metrics: RankingMetrics


@dataclass(frozen=True)
class TrainingRun:
    """All candidates and the deterministic recommendation from one snapshot."""

    lexical_baseline: RankingMetrics
    candidates: tuple[TrainingCandidate, ...]
    recommended: TrainingCandidate


class TrainingPipeline:
    """Train interchangeable rankers on a caller-owned bounded snapshot."""

    def __init__(self, catalog: RankerCatalog | None = None, split: TrainingSplit = TrainingSplit()) -> None:
        self.catalog = catalog or RankerCatalog((GradeRegressionRanker(), PairwiseLinearRanker()))
        self.split = split

    def run(self, rows: Sequence[TrainingExample], *, snapshot_id: str) -> TrainingRun:
        if not snapshot_id:
            raise ValueError("Training runs require a nonblank snapshot_id.")
        train, validation = self.split.partition(rows)
        baseline = _evaluate(validation, lambda row: row.features.get("lexical_score", 0.0))
        candidates = []
        for trainer in self.catalog.trainers:
            provisional = trainer.fit(
                train,
                snapshot_id=snapshot_id,
                split_seed=self.split.seed,
                validation=RankingMetrics(0.0, 0.0, 0.0, 0, 0),
            )
            scorer = LinearArtifactScorer(provisional)
            metrics = _evaluate(validation, lambda row: scorer.score(row.features))
            artifact = trainer.fit(train, snapshot_id=snapshot_id, split_seed=self.split.seed, validation=metrics)
            candidates.append(TrainingCandidate(artifact, metrics))
        if not candidates:
            raise ValueError("Training run requires at least one registered ranker.")
        ordered = tuple(sorted(candidates, key=_recommendation_key))
        return TrainingRun(lexical_baseline=baseline, candidates=tuple(candidates), recommended=ordered[0])


def _recommendation_key(candidate: TrainingCandidate) -> tuple[float, float, float, str]:
    metrics = candidate.metrics
    return (-metrics.ndcg_at_10, -metrics.ndcg_at_5, -metrics.mean_reciprocal_rank, candidate.artifact.ranker_id)


def _evaluate(rows: Sequence[TrainingExample], score) -> RankingMetrics:
    by_query: dict[str, list[TrainingExample]] = {}
    for row in rows:
        by_query.setdefault(row.query_id, []).append(row)
    ndcg5, ndcg10, reciprocal, covered = [], [], [], 0
    for query_rows in by_query.values():
        ranked = sorted(query_rows, key=lambda row: (-score(row), row.document_id))
        ideal = sorted(query_rows, key=lambda row: (-row.relevance, row.document_id))
        ndcg5.append(_ndcg(ranked, ideal, 5))
        ndcg10.append(_ndcg(ranked, ideal, 10))
        relevant = next((index for index, row in enumerate(ranked, 1) if row.relevance >= 2.0), None)
        reciprocal.append(0.0 if relevant is None else 1.0 / relevant)
        covered += 1
    count = len(by_query)
    if not count:
        return RankingMetrics(0.0, 0.0, 0.0, 0, 0)
    return RankingMetrics(sum(ndcg5) / count, sum(ndcg10) / count, sum(reciprocal) / count, count, covered)


def _ndcg(ranked: Sequence[TrainingExample], ideal: Sequence[TrainingExample], cutoff: int) -> float:
    actual = _dcg(ranked[:cutoff])
    maximum = _dcg(ideal[:cutoff])
    return 0.0 if maximum == 0.0 else actual / maximum


def _dcg(rows: Sequence[TrainingExample]) -> float:
    return sum((2.0**row.relevance - 1.0) / log2(index + 1.0) for index, row in enumerate(rows, 1))
