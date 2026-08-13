"""Rank corpus similarity from canonical lexical-similarity pairs."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DocumentSimilaritySearch:
    """Choose the directed score that starts at each query document."""

    @staticmethod
    def rank(
        query: DataFrame, documents: DataFrame, similarities: DataFrame, limit: int, declared: DataFrame
    ) -> DataFrame:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        query_ids = query.select(F.col("id").alias("query_id"))
        candidates = similarities.join(
            query_ids,
            (F.col("left_document_id") == F.col("query_id")) | (F.col("right_document_id") == F.col("query_id")),
        ).select(
            "query_id",
            F.when(F.col("left_document_id") == F.col("query_id"), F.col("right_document_id"))
            .otherwise(F.col("left_document_id"))
            .alias("document_id"),
            "score_overlap",
            F.when(F.col("left_document_id") == F.col("query_id"), F.col("bm25_left_to_right"))
            .otherwise(F.col("bm25_right_to_left"))
            .alias("score_bm25"),
        )
        ranked = candidates.withColumn(
            "rank",
            F.row_number()
            .over(
                Window.partitionBy("query_id").orderBy(
                    F.col("score_bm25").desc(),
                    F.col("score_overlap").desc(),
                    F.col("document_id").asc(),
                )
            )
            .cast("long"),
        ).where(F.col("rank") <= F.lit(limit))
        ranked = ranked.alias("ranked")
        document = documents.alias("document")
        return (
            ranked.join(document, F.col("ranked.document_id") == F.col("document.id"))
            .select(
                F.col("document.id").alias("id"),
                F.col("document.collection_id").alias("collection_id"),
                F.col("document.source").alias("source"),
                F.col("document.title").alias("title"),
                F.col("document.url").alias("url"),
                F.col("document.content").alias("content"),
                F.col("document.content_type").alias("content_type"),
                F.col("document.encoding").alias("encoding"),
                F.col("document.language").alias("language"),
                F.col("document.created_at").alias("created_at"),
                F.col("document.published_at").alias("published_at"),
                F.col("document.harvested_at").alias("harvested_at"),
                F.col("ranked.query_id").alias("search_query_id"),
                F.col("ranked.score_overlap").alias("score_overlap"),
                F.col("ranked.score_bm25").alias("score_bm25"),
                F.col("ranked.rank").alias("rank"),
            )
            .select(*declared.columns)
        )
