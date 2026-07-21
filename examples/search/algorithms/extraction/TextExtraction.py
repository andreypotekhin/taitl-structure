"""PySpark-only row expansion kept behind the search example's raw boundary."""


class TextExtraction:
    """Build hierarchy DataFrames with Spark-native expressions, not Python UDFs."""

    @staticmethod
    def hierarchy(documents, *, sections, paragraphs, sentences, words):
        """Replace each declared output with its expanded hierarchy rows."""

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        lines = documents.select(
            "id",
            F.posexplode(F.split(F.regexp_replace("content", "\\r\\n?", "\\n"), "\\n")).alias("line_ordinal", "line"),
        )
        heading = F.trim(F.regexp_extract("line", r"^\s*#+\s+(.+?)\s*$", 1))
        marked = lines.withColumn("heading", F.when(F.col("line").rlike(r"^\s*#+\s+"), heading)).withColumn(
            "is_blank", F.trim("line") == ""
        )
        document_window = Window.partitionBy("id").orderBy("line_ordinal").rowsBetween(Window.unboundedPreceding, 0)
        marked = marked.withColumn(
            "section_ordinal", F.sum(F.when(F.col("heading").isNotNull(), 1).otherwise(0)).over(document_window)
        ).withColumn("paragraph_group", F.sum(F.when(F.col("is_blank"), 1).otherwise(0)).over(document_window))
        section_rows, paragraph_rows = TextExtraction._sections_and_paragraphs(marked)
        sentence_rows, word_rows = TextExtraction._sentences_and_words(paragraph_rows)
        return tuple(
            rows.select(*declared.columns)
            for rows, declared in zip(
                (section_rows, paragraph_rows, sentence_rows, word_rows),
                (sections, paragraphs, sentences, words),
                strict=True,
            )
        )

    @staticmethod
    def _sections_and_paragraphs(marked):
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        body = marked.where(~F.col("is_blank") & F.col("heading").isNull())
        lines_per_paragraph = body.groupBy("id", "section_ordinal", "paragraph_group").agg(
            F.sort_array(F.collect_list(F.struct("line_ordinal", "line"))).alias("ordered_lines")
        )
        paragraphs = lines_per_paragraph.select(
            F.concat_ws("#p", "id", F.col("paragraph_group").cast("string")).alias("id"),
            F.col("id").alias("document_id"),
            F.concat_ws("#s", "id", F.col("section_ordinal").cast("string")).alias("section_id"),
            F.row_number()
            .over(Window.partitionBy("id", "section_ordinal").orderBy("paragraph_group"))
            .alias("ordinal"),
            F.concat_ws(" ", F.transform("ordered_lines", lambda row: row["line"])).alias("content"),
            F.lit(None).cast("string").alias("search_query_id"),
            F.lit(None).cast("double").alias("score_overlap"),
            F.lit(None).cast("double").alias("score_bm25"),
        )
        headings = (
            marked.where(F.col("heading").isNotNull())
            .select(F.col("id").alias("heading_document_id"), "section_ordinal", "heading")
            .alias("headings")
        )
        section_keys = paragraphs.select("document_id", "section_id").distinct().alias("section_keys")
        sections = section_keys.join(
            headings,
            (F.col("section_keys.document_id") == F.col("headings.heading_document_id"))
            & (
                F.regexp_extract(F.col("section_keys.section_id"), r"#s([0-9]+)$", 1).cast("int")
                == F.col("headings.section_ordinal")
            ),
            "left",
        ).select(
            F.col("section_keys.section_id").alias("id"),
            F.col("section_keys.document_id").alias("document_id"),
            F.regexp_extract(F.col("section_keys.section_id"), r"#s([0-9]+)$", 1).cast("int").alias("ordinal"),
            F.coalesce(F.col("headings.heading"), F.lit("Document")).alias("heading"),
            F.lit(None).cast("string").alias("search_query_id"),
            F.lit(None).cast("double").alias("score_overlap"),
            F.lit(None).cast("double").alias("score_bm25"),
        )
        return sections, paragraphs

    @staticmethod
    def _sentences_and_words(paragraphs):
        from pyspark.sql import functions as F

        sentences = (
            paragraphs.select(
                "document_id",
                "section_id",
                F.col("id").alias("paragraph_id"),
                F.col("ordinal").alias("paragraph_ordinal"),
                F.posexplode(F.split("content", r"(?<=[.!?])\s+")).alias("position", "content"),
            )
            .where(F.trim("content") != "")
            .select(
                F.concat_ws("#s", "paragraph_id", F.col("position").cast("string")).alias("id"),
                "document_id",
                "section_id",
                "paragraph_id",
                "paragraph_ordinal",
                (F.col("position") + 1).cast("int").alias("ordinal"),
                F.trim("content").alias("content"),
                F.lit(None).cast("string").alias("search_query_id"),
                F.lit(None).cast("double").alias("score_overlap"),
                F.lit(None).cast("double").alias("score_bm25"),
            )
        )
        words = (
            sentences.select(
                "document_id",
                "section_id",
                "paragraph_id",
                "paragraph_ordinal",
                F.col("id").alias("sentence_id"),
                F.posexplode(F.split("content", r"\s+")).alias("position", "token"),
            )
            .where(F.trim("token") != "")
            .select(
                F.concat_ws("#w", "sentence_id", F.col("position").cast("string")).alias("id"),
                "document_id",
                "section_id",
                "paragraph_id",
                "paragraph_ordinal",
                "sentence_id",
                (F.col("position") + 1).cast("int").alias("ordinal"),
                F.lower(F.regexp_replace(F.trim("token"), r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "")).alias("token"),
            )
            .where(F.col("token") != "")
        )
        return sentences, words
