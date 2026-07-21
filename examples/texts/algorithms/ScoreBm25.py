"""BM25 search scoring."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from examples.texts.algorithms.TextSearch import ScoreFrames, Target, TextSearch

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ScoreBm25(TextSearch):
    """Calculate BM25 scores with fixed standard tuning constants."""

    _K1: Final = 1.2
    _B: Final = 0.75

    @classmethod
    def scores(
        cls,
        queries: DataFrame,
        words: DataFrame,
        *,
        document_scores: DataFrame,
        section_scores: DataFrame,
        paragraph_scores: DataFrame,
        sentence_scores: DataFrame,
    ) -> ScoreFrames:
        """Return scores projected through the transform's four output contracts."""

        query_terms = cls.query_terms(queries)
        scores = tuple(cls._scores(query_terms, words, target) for target in cls._TARGETS)
        return cls.project_scores(scores, (document_scores, section_scores, paragraph_scores, sentence_scores))

    @classmethod
    def _scores(cls, query_terms: DataFrame, words: DataFrame, target: Target) -> DataFrame:
        from pyspark.sql import functions as F

        terms, targets = cls.index(words, target)
        frequencies = terms.groupBy("token").agg(F.count("*").alias("_document_frequency"))
        corpus = targets.agg(
            F.count("*").alias("_target_count"),
            F.avg("_word_count").alias("_average_target_length"),
        )
        matches = (
            query_terms.join(terms, "token").join(frequencies, "token").join(targets, list(target)).crossJoin(corpus)
        )
        inverse_frequency = F.log1p(
            (F.col("_target_count") - F.col("_document_frequency") + F.lit(0.5))
            / (F.col("_document_frequency") + F.lit(0.5))
        )
        normalization = F.col("_term_frequency") + F.lit(cls._K1) * (
            F.lit(1 - cls._B) + F.lit(cls._B) * F.col("_word_count") / F.col("_average_target_length")
        )
        return (
            matches.withColumn(
                "_bm25_term",
                inverse_frequency * F.col("_term_frequency") * F.lit(cls._K1 + 1) / normalization,
            )
            .groupBy("query_id", *target)
            .agg(F.sum("_bm25_term").alias("score_bm25"))
            .select("query_id", *target, "score_bm25")
        )
