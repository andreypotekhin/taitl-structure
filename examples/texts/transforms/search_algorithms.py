"""Spark-native scoring algorithms used by the texts search transforms."""


class TextSearch:
    """Shared query normalization and target-level index construction."""

    _TARGETS = (
        ("document_id",),
        ("document_id", "section_id"),
        ("document_id", "section_id", "paragraph_id"),
        ("document_id", "section_id", "paragraph_id", "sentence_id"),
    )

    @staticmethod
    def query_terms(queries, functions):
        terms = functions.posexplode(functions.split(functions.trim("content"), r"\s+")).alias("_position", "token")
        token = functions.lower(functions.regexp_replace(functions.trim("token"), r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", ""))
        return (
            queries.select(functions.col("id").alias("query_id"), terms)
            .select("query_id", token.alias("token"))
            .where(functions.col("token") != "")
            .distinct()
        )

    @staticmethod
    def index(words, target, functions):
        terms = words.groupBy(*target, "token").agg(functions.count("*").alias("_term_frequency"))
        targets = words.groupBy(*target).agg(
            functions.count("*").alias("_word_count"),
            functions.countDistinct("token").alias("_distinct_terms"),
        )
        return terms, targets


class ScoreOverlap(TextSearch):
    """Calculate the overlap coefficient for each query and target."""

    @classmethod
    def scores(cls, queries, words):
        from pyspark.sql import functions as functions

        query_terms = cls.query_terms(queries, functions)
        query_sizes = query_terms.groupBy("query_id").agg(functions.count("*").alias("_query_terms"))
        return tuple(cls._scores(query_terms, query_sizes, words, target, functions) for target in cls._TARGETS)

    @classmethod
    def _scores(cls, query_terms, query_sizes, words, target, functions):
        terms, targets = cls.index(words, target, functions)
        return (
            query_terms.join(terms, "token")
            .join(targets, list(target))
            .join(query_sizes, "query_id")
            .groupBy("query_id", *target, "_query_terms", "_distinct_terms")
            .agg(functions.countDistinct("token").alias("_matched_terms"))
            .select(
                "query_id",
                *target,
                (
                    functions.col("_matched_terms")
                    / functions.least(functions.col("_query_terms"), functions.col("_distinct_terms"))
                ).alias("score_overlap"),
            )
        )


class ScoreBm25(TextSearch):
    """Calculate BM25 scores with fixed standard tuning constants."""

    _K1 = 1.2
    _B = 0.75

    @classmethod
    def scores(cls, queries, words):
        from pyspark.sql import functions as functions

        query_terms = cls.query_terms(queries, functions)
        return tuple(cls._scores(query_terms, words, target, functions) for target in cls._TARGETS)

    @classmethod
    def _scores(cls, query_terms, words, target, functions):
        terms, targets = cls.index(words, target, functions)
        frequencies = terms.groupBy("token").agg(functions.count("*").alias("_document_frequency"))
        corpus = targets.agg(
            functions.count("*").alias("_target_count"),
            functions.avg("_word_count").alias("_average_target_length"),
        )
        matches = (
            query_terms.join(terms, "token").join(frequencies, "token").join(targets, list(target)).crossJoin(corpus)
        )
        inverse_frequency = functions.log1p(
            (functions.col("_target_count") - functions.col("_document_frequency") + functions.lit(0.5))
            / (functions.col("_document_frequency") + functions.lit(0.5))
        )
        normalization = functions.col("_term_frequency") + functions.lit(cls._K1) * (
            functions.lit(1 - cls._B)
            + functions.lit(cls._B) * functions.col("_word_count") / functions.col("_average_target_length")
        )
        return (
            matches.withColumn(
                "_bm25_term",
                inverse_frequency * functions.col("_term_frequency") * functions.lit(cls._K1 + 1) / normalization,
            )
            .groupBy("query_id", *target)
            .agg(functions.sum("_bm25_term").alias("score_bm25"))
            .select("query_id", *target, "score_bm25")
        )
