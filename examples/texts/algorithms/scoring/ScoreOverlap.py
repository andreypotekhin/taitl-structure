"""Overlap-coefficient scoring over reusable index terms."""

from __future__ import annotations

from typing import TYPE_CHECKING

from examples.texts.algorithms.scoring.ScoreAlgorithm import ScoreAlgorithm, ScoreFrames

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ScoreOverlap(ScoreAlgorithm):
    """Calculate overlap scores from four target-grain index tables."""

    @classmethod
    def scores(cls, queries: DataFrame, terms: ScoreFrames, declared: ScoreFrames) -> ScoreFrames:
        from pyspark.sql import functions as F

        query_terms = cls.query_terms(queries)
        query_sizes = query_terms.groupBy("query_id").agg(F.count("*").alias("_query_terms"))
        scores = tuple(
            cls._scores(query_terms, query_sizes, terms, target)
            for terms, target in zip(terms, cls._TARGETS, strict=True)
        )
        return cls.project_scores(scores, declared)

    @staticmethod
    def _scores(query_terms: DataFrame, query_sizes: DataFrame, terms: DataFrame, target: tuple[str, ...]) -> DataFrame:
        from pyspark.sql import functions as F

        return (
            query_terms.join(terms, "token")
            .join(query_sizes, "query_id")
            .groupBy("query_id", *target, "_query_terms", "target_distinct_terms")
            .agg(F.countDistinct("token").alias("_matched_terms"))
            .select(
                "query_id",
                *target,
                (F.col("_matched_terms") / F.least(F.col("_query_terms"), F.col("target_distinct_terms"))).alias(
                    "score_overlap"
                ),
            )
        )
