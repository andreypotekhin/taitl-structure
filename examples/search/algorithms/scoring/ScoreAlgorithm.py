"""Shared query normalization and index-backed scoring helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


Target: TypeAlias = tuple[str, ...]
ScoreFrames: TypeAlias = tuple["DataFrame", ...]


class ScoreAlgorithm:
    """Normalize queries and project Spark results through declared schemas."""

    _TARGETS: tuple[Target, ...] = (
        ("document_id",),
        ("document_id", "section_id"),
        ("document_id", "section_id", "paragraph_id"),
        ("document_id", "section_id", "paragraph_id", "sentence_id"),
    )

    @staticmethod
    def query_terms(queries: DataFrame) -> DataFrame:
        """Normalize each query into distinct, non-empty terms."""

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
    def project_scores(scores: ScoreFrames, declared: ScoreFrames) -> ScoreFrames:
        return tuple(score.select(*output.columns) for score, output in zip(scores, declared, strict=True))
