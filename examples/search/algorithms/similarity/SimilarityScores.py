"""Reduce directed reusable-index scores into reciprocal canonical pairs."""

from __future__ import annotations

from typing import Final


class SimilarityScores:
    """Keep same-grain, non-self matches with lexical evidence in both directions."""

    _TARGETS: Final = (
        ("document_id",),
        ("document_id", "section_id"),
        ("document_id", "section_id", "paragraph_id"),
        ("document_id", "section_id", "paragraph_id", "sentence_id"),
    )

    @classmethod
    def reduce(cls, mappings, overlaps, bm25_scores, declared):
        pairs = tuple(
            cls._pairs(mapping, overlap, bm25, target)
            for mapping, overlap, bm25, target in zip(mappings, overlaps, bm25_scores, cls._TARGETS, strict=True)
        )
        return tuple(frame.select(*output.columns) for frame, output in zip(pairs, declared, strict=True))

    @staticmethod
    def _pairs(mapping, overlap, bm25, target: tuple[str, ...]):
        from pyspark.sql import functions as F

        score = overlap.join(bm25, ["query_id", *target])
        source = mapping.alias("source")
        target_score = score.alias("target")
        directed = source.join(target_score, F.col("source.query_id") == F.col("target.query_id")).select(
            *(F.col(f"source.{field}").alias(f"source_{field}") for field in target),
            *(F.col(f"target.{field}").alias(f"target_{field}") for field in target),
            F.col("target.score_overlap").alias("score_overlap"),
            F.col("target.score_bm25").alias("score_bm25"),
        )
        identity = target[-1]
        forward = directed.where(F.col(f"source_{identity}") < F.col(f"target_{identity}")).alias("forward")
        reverse = directed.alias("reverse")
        conditions = [F.col(f"forward.source_{field}") == F.col(f"reverse.target_{field}") for field in target] + [
            F.col(f"forward.target_{field}") == F.col(f"reverse.source_{field}") for field in target
        ]
        condition = conditions[0]
        for item in conditions[1:]:
            condition = condition & item
        return forward.join(reverse, condition).select(
            *(F.col(f"forward.source_{field}").alias(f"left_{field}") for field in target),
            *(F.col(f"forward.target_{field}").alias(f"right_{field}") for field in target),
            F.least(F.col("forward.score_overlap"), F.col("reverse.score_overlap")).alias("score_overlap"),
            F.col("forward.score_bm25").alias("bm25_left_to_right"),
            F.col("reverse.score_bm25").alias("bm25_right_to_left"),
            ((F.col("forward.score_bm25") + F.col("reverse.score_bm25")) / F.lit(2.0)).alias("bm25_mean"),
        )
