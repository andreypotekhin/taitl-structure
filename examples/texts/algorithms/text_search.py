"""Shared query normalization and target-level index construction."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


Target: TypeAlias = tuple[str, ...]
ScoreFrames: TypeAlias = tuple["DataFrame", ...]


class TextSearch:
    """Build normalized query terms and independent hierarchy indexes."""

    _TARGETS: tuple[Target, ...] = (
        ("document_id",),
        ("document_id", "section_id"),
        ("document_id", "section_id", "paragraph_id"),
        ("document_id", "section_id", "paragraph_id", "sentence_id"),
    )

    @staticmethod
    def query_terms(queries: DataFrame) -> DataFrame:
        """Normalize each query into its distinct non-empty terms."""

        from pyspark.sql import functions as F

        terms = F.posexplode(F.split(F.trim("content"), r"\s+")).alias("_position", "token")
        token = F.lower(F.regexp_replace(F.trim("token"), r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", ""))
        return (
            queries.select(F.col("id").alias("query_id"), terms)
            .select("query_id", token.alias("token"))
            .where(F.col("token") != "")
            .distinct()
        )

    @staticmethod
    def index(words: DataFrame, target: Target) -> tuple[DataFrame, DataFrame]:
        """Return token frequencies and length facts for one target grain."""

        from pyspark.sql import functions as F

        terms = words.groupBy(*target, "token").agg(F.count("*").alias("_term_frequency"))
        targets = words.groupBy(*target).agg(
            F.count("*").alias("_word_count"),
            F.countDistinct("token").alias("_distinct_terms"),
        )
        return terms, targets

    @staticmethod
    def project_scores(scores: ScoreFrames, declared: ScoreFrames) -> ScoreFrames:
        """Project replacement scores through the hook's declared output shapes."""

        return tuple(score.select(*output.columns) for score, output in zip(scores, declared, strict=True))
