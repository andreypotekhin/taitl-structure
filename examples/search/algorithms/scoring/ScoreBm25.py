"""BM25 scoring over reusable index terms and summaries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from examples.search.algorithms.scoring.ScoreAlgorithm import ScoreAlgorithm, ScoreFrames

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ScoreBm25(ScoreAlgorithm):
    """Calculate BM25 scores with standard fixed tuning constants."""

    _K1: Final = 1.2
    _B: Final = 0.75

    @classmethod
    def scores(
        cls, queries: DataFrame, terms: ScoreFrames, summaries: ScoreFrames, declared: ScoreFrames
    ) -> ScoreFrames:
        query_terms = cls.query_terms(queries)
        scores = tuple(
            cls._scores(query_terms, terms, summary, target)
            for terms, summary, target in zip(terms, summaries, cls._TARGETS, strict=True)
        )
        return cls.project_scores(scores, declared)

    @classmethod
    def _scores(
        cls, query_terms: DataFrame, terms: DataFrame, summary: DataFrame, target: tuple[str, ...]
    ) -> DataFrame:
        from pyspark.sql import functions as F

        matches = query_terms.join(terms, "token").crossJoin(summary)
        inverse_frequency = F.log1p(
            (F.col("target_count") - F.col("document_frequency") + F.lit(0.5))
            / (F.col("document_frequency") + F.lit(0.5))
        )
        normalization = F.col("term_frequency") + F.lit(cls._K1) * (
            F.lit(1 - cls._B) + F.lit(cls._B) * F.col("target_word_count") / F.col("average_target_length")
        )
        return (
            matches.withColumn(
                "_bm25_term",
                inverse_frequency * F.col("term_frequency") * F.lit(cls._K1 + 1) / normalization,
            )
            .groupBy("query_id", *target)
            .agg(F.sum("_bm25_term").alias("score_bm25"))
            .select("query_id", *target, "score_bm25")
        )
