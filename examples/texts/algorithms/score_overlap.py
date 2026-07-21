"""Overlap-coefficient search scoring."""

from __future__ import annotations

from typing import TYPE_CHECKING

from examples.texts.algorithms.text_search import ScoreFrames, Target, TextSearch

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ScoreOverlap(TextSearch):
    """Calculate the overlap coefficient for each query and target."""

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

        from pyspark.sql import functions as F

        query_terms = cls.query_terms(queries)
        query_sizes = query_terms.groupBy("query_id").agg(F.count("*").alias("_query_terms"))
        scores = tuple(cls._scores(query_terms, query_sizes, words, target) for target in cls._TARGETS)
        return cls.project_scores(scores, (document_scores, section_scores, paragraph_scores, sentence_scores))

    @classmethod
    def _scores(cls, query_terms: DataFrame, query_sizes: DataFrame, words: DataFrame, target: Target) -> DataFrame:
        from pyspark.sql import functions as F

        terms, targets = cls.index(words, target)
        return (
            query_terms.join(terms, "token")
            .join(targets, list(target))
            .join(query_sizes, "query_id")
            .groupBy("query_id", *target, "_query_terms", "_distinct_terms")
            .agg(F.countDistinct("token").alias("_matched_terms"))
            .select(
                "query_id",
                *target,
                (F.col("_matched_terms") / F.least(F.col("_query_terms"), F.col("_distinct_terms"))).alias(
                    "score_overlap"
                ),
            )
        )
