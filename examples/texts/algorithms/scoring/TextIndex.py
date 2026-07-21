"""Reusable four-grain inverted-index construction."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


Target: TypeAlias = tuple[str, ...]
IndexFrames: TypeAlias = tuple["DataFrame", ...]


class TextIndex:
    """Build target-local term facts and corpus summaries from word rows."""

    _TARGETS: tuple[Target, ...] = (
        ("document_id",),
        ("document_id", "section_id"),
        ("document_id", "section_id", "paragraph_id"),
        ("document_id", "section_id", "paragraph_id", "sentence_id"),
    )

    @classmethod
    def build(cls, words: DataFrame, declared: IndexFrames) -> IndexFrames:
        """Build term and summary artifacts in document-to-sentence order."""

        pairs = tuple(cls._index(words, target) for target in cls._TARGETS)
        actual = tuple(frame for pair in pairs for frame in pair)
        return tuple(frame.select(*output.columns) for frame, output in zip(actual, declared, strict=True))

    @staticmethod
    def _index(words: DataFrame, target: Target) -> tuple[DataFrame, DataFrame]:
        from pyspark.sql import functions as F

        terms = words.groupBy(*target, "token").agg(F.count("*").alias("term_frequency"))
        targets = words.groupBy(*target).agg(
            F.count("*").alias("target_word_count"),
            F.countDistinct("token").alias("target_distinct_terms"),
        )
        frequencies = terms.groupBy("token").agg(F.count("*").alias("document_frequency"))
        return (
            terms.join(targets, list(target)).join(frequencies, "token"),
            targets.agg(
                F.count("*").alias("target_count"),
                F.coalesce(F.avg("target_word_count"), F.lit(0.0)).alias("average_target_length"),
            ),
        )
