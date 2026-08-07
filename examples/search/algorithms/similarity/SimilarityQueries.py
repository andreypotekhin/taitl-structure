"""Build tagged self-queries from reusable text-index terms."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SimilarityQueries:
    """Create same-grain queries whose vocabularies exactly match index targets."""

    _TARGETS: Final = (
        ("document", ("document_id",)),
        ("section", ("document_id", "section_id")),
        ("paragraph", ("document_id", "section_id", "paragraph_id")),
        ("sentence", ("document_id", "section_id", "paragraph_id", "sentence_id")),
    )

    @classmethod
    def build(cls, terms, summaries, policy: DataFrame, declared):
        """Return query and target-mapping artifacts in document-to-sentence order."""

        ratio = cls._ratio(policy)
        pairs = tuple(
            cls._queries(name, target, term, summary, ratio)
            for (name, target), term, summary in zip(cls._TARGETS, terms, summaries, strict=True)
        )
        queries = pairs[0][0]
        for query, _ in pairs[1:]:
            queries = queries.unionByName(query)
        queries = cls._as_search_queries(queries)
        actual = (queries, *(mapping for _, mapping in pairs))
        return tuple(frame.select(*output.columns) for frame, output in zip(actual, declared, strict=True))

    @staticmethod
    def _as_search_queries(queries: DataFrame) -> DataFrame:
        """Add the required default classification fields to generated self-queries."""

        from pyspark.sql import functions as F

        return (
            queries.withColumn(
                "labels",
                F.create_map(
                    F.lit("is_question"),
                    F.lit(0).cast("long"),
                    F.lit("is_time_sensitive"),
                    F.lit(0).cast("long"),
                ),
            )
            .withColumn("is_question", F.lit(False))
            .withColumn("is_time_sensitive", F.lit(False))
            .withColumn("language", F.lit(None).cast("string"))
        )

    @staticmethod
    def _ratio(policy: DataFrame) -> float | None:
        rows = policy.select("max_document_frequency_ratio").limit(2).collect()
        if len(rows) != 1:
            raise ValueError("SimilarityPolicy must contain exactly one row")
        ratio = rows[0]["max_document_frequency_ratio"]
        if ratio is not None and not 0 < ratio <= 1:
            raise ValueError("SimilarityPolicy.max_document_frequency_ratio must be in (0, 1]")
        return ratio

    @staticmethod
    def _queries(name: str, target: tuple[str, ...], terms, summary, ratio: float | None):
        from pyspark.sql import functions as F

        retained = terms
        if ratio is not None:
            retained = retained.crossJoin(summary.select("target_count")).where(
                F.col("target_frequency") / F.col("target_count") <= F.lit(ratio)
            )
        targets = retained.groupBy(*target).agg(
            F.array_join(F.sort_array(F.collect_list("token")), " ").alias("content")
        )
        identity = target[-1]
        query_id = F.concat(F.lit(f"{name}:"), F.col(identity))
        query_targets = targets.select(query_id.alias("query_id"), *(F.col(column) for column in target), "content")
        mapping = query_targets.drop("content")
        return query_targets.select(F.col("query_id").alias("id"), "content"), mapping
