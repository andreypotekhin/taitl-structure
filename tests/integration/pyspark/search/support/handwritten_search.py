"""Independent handwritten reference for the cached lexical Search fixture."""

from __future__ import annotations

from typing import Any


def cached_lexical_search(inputs: dict[str, Any], prepared: dict[str, Any] | None = None) -> Any:
    """Reproduce the fixture's cached lexical retrieval, fusion, and reranking in PySpark.

    This deliberately narrow reference requires empty vector and streamed-candidate inputs. It is an
    independently expressed oracle for the integration fixture, not a production Search implementation.
    """

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    scores = inputs["document_scores"].alias("score")
    documents = (prepared["documents"] if prepared is not None else inputs["documents"]).alias("document")
    queries = inputs["queries"].alias("query")
    requests = inputs["requests"].alias("request")
    targets = inputs["document_filter_targets"].alias("target")
    bands = inputs["band_memberships"].alias("band")

    selected = (
        scores.join(documents, F.col("document.id") == F.col("score.document_id"))
        .join(queries, F.col("query.id") == F.col("score.query_id"))
        .join(requests, (F.col("request.query_id") == F.col("query.id")))
        .join(
            targets,
            (F.col("target.query_id") == F.col("query.id")) & (F.col("target.document_id") == F.col("document.id")),
        )
        .join(bands, F.col("band.user_id") == F.col("request.user_id"), "left")
        .where(
            F.col("score.score").isNotNull()
            & F.col("score.experiment_id").eqNullSafe(F.col("request.experiment_id"))
            & (F.col("query.requested_at") == F.col("request.requested_at"))
        )
        .select(
            F.col("query.id").alias("search_query_id"),
            F.col("score.experiment_id"),
            F.col("band.user_band_id"),
            F.col("band.band_id"),
            F.lower(F.regexp_replace(F.trim(F.col("query.content")), r"\s+", " ")).alias("query"),
            F.lit(0).cast("long").alias("candidate_rank"),
            F.col("document.id").alias("document_id"),
            F.when(F.col("score.score").isNotNull(), F.col("document.title"))
            .otherwise(F.lit(None).cast("string"))
            .alias("title"),
            F.col("document.url"),
            F.when(F.col("score.score").isNotNull(), F.col("score.score"))
            .otherwise(F.lit(None).cast("double"))
            .alias("score"),
            F.when(F.col("score.score").isNotNull(), F.col("score.score"))
            .otherwise(F.lit(None).cast("double"))
            .alias("retrieval_score"),
            F.lit(0.0).alias("score_feedback"),
            F.lit(0.0).alias("score_rank"),
            F.lit(0.0).alias("score_weight"),
            F.lit(0.0).alias("feedback_weight"),
            F.lit(None).cast("long").alias("lexical_rank"),
            F.lit(None).cast("long").alias("vector_rank"),
            F.lit(None).cast("double").alias("vector_similarity"),
            F.lit(0.0).alias("rrf_score"),
            F.lit(0).cast("long").alias("rrf_k"),
            F.lit(None).cast("string").alias("vector_backend"),
        )
    )

    uniqueness_key = ["search_query_id", "user_band_id", "experiment_id", "document_id"]
    duplicate = selected.groupBy(*uniqueness_key).count().where(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise ValueError("The cached lexical fixture violates Search candidate uniqueness.")

    lexical_order = Window.partitionBy("search_query_id", "user_band_id", "experiment_id").orderBy(
        F.col("score").desc_nulls_last(), F.col("document_id").asc_nulls_first()
    )
    ranked = selected.withColumn("candidate_rank", F.row_number().over(lexical_order).cast("long")).withColumn(
        "lexical_rank",
        F.when(F.col("score").isNotNull(), F.col("candidate_rank")).otherwise(F.lit(None).cast("long")),
    )
    # This fixture has one fixed policy row and no vector candidates. Carry only its scalar RRF
    # constant; crossing the whole policy frame would duplicate `experiment_id` and other names.
    rrf_k = prepared["rrf_k"] if prepared is not None else inputs["vector_policy"].first()["rrf_k"]
    scored = ranked.withColumn("rrf_k", F.lit(rrf_k).cast("long")).withColumn(
        "rrf_score", F.coalesce(F.lit(1.0) / (F.col("rrf_k") + F.col("lexical_rank")), F.lit(0.0))
    )

    candidates = scored.where(F.col("candidate_rank") <= 1000).alias("candidate")
    relevance = prepared["relevance"] if prepared is not None else inputs["policy"].first()
    minimum_band_impressions = relevance["minimum_band_impressions"]
    feedback_band = F.lit(None).cast("string")
    query_feedback = inputs["query_document_signals"].alias("signal")
    popularity = prepared["document_popularity"] if prepared is not None else inputs["document_popularity"]
    popularity_feedback = popularity.alias("popularity")

    query_match = (
        (F.col("signal.query") == F.col("candidate.query"))
        & (F.col("signal.document_id") == F.col("candidate.document_id"))
        & F.col("signal.band_id").eqNullSafe(feedback_band)
        & (F.col("signal.band_id").isNull() | (F.col("signal.impression_count") >= minimum_band_impressions))
    )
    popularity_match = (
        (F.col("popularity.document_id") == F.col("candidate.document_id"))
        & F.col("popularity.band_id").eqNullSafe(feedback_band)
        & (F.col("popularity.band_id").isNull() | (F.col("popularity.impression_count") >= minimum_band_impressions))
    )
    feedback = (
        candidates.join(query_feedback, query_match, "left")
        .join(popularity_feedback, popularity_match, "left")
        .select(
            F.col("candidate.*"),
            F.col("signal.normalized_score").alias("query_signal_score"),
            F.col("popularity.normalized_score").alias("popularity_score"),
        )
    )
    weighted = (
        feedback.withColumn(
            "score_feedback",
            0.8 * F.coalesce(F.col("query_signal_score"), F.lit(0.0))
            + 0.2 * F.coalesce(F.col("popularity_score"), F.lit(0.0)),
        )
        .withColumn("score_weight", F.lit(relevance["score_weight"]))
        .withColumn("feedback_weight", F.lit(relevance["feedback_weight"]))
    )

    score_window = Window.partitionBy("search_query_id", "user_band_id", "experiment_id")
    ranked_results = (
        weighted.withColumn("maximum_score", F.max("retrieval_score").over(score_window))
        .withColumn(
            "score_rank",
            F.col("score_weight") * F.col("score") / F.col("maximum_score")
            + F.col("feedback_weight") * F.col("score_feedback"),
        )
        .withColumn(
            "rank",
            F.row_number()
            .over(
                Window.partitionBy("search_query_id", "user_band_id", "experiment_id").orderBy(
                    F.col("score_rank").desc_nulls_last(), F.col("document_id").asc_nulls_first()
                )
            )
            .cast("long"),
        )
        .where(F.col("rank") <= 100)
    )
    return ranked_results.select(
        "search_query_id",
        "experiment_id",
        "user_band_id",
        "band_id",
        "rank",
        "candidate_rank",
        "document_id",
        "title",
        "url",
        "score",
        "retrieval_score",
        "score_feedback",
        "score_rank",
        "lexical_rank",
        "vector_rank",
        "vector_similarity",
        "rrf_score",
        "rrf_k",
        "vector_backend",
    )


def prepare_cached_lexical_search(inputs: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], tuple[Any, ...]]:
    """Materialize fixed fixture dimensions and hoist scalar policies for repeated handwritten requests.

    The returned cached frames are caller-owned and must be unpersisted by the caller after the benchmark sequence.
    This is only the cached-score lexical fixture, not reusable preparation for full Search.
    """

    prepared_inputs = dict(inputs)
    prepared_inputs["documents"] = inputs["documents"].cache()
    prepared_inputs["document_popularity"] = inputs["document_popularity"].cache()
    cached_frames = (prepared_inputs["documents"], prepared_inputs["document_popularity"])
    try:
        for frame in cached_frames:
            frame.count()
        prepared = {
            "rrf_k": inputs["vector_policy"].first()["rrf_k"],
            "relevance": inputs["policy"].first(),
            "documents": prepared_inputs["documents"],
            "document_popularity": prepared_inputs["document_popularity"],
        }
    except Exception:
        for frame in cached_frames:
            frame.unpersist()
        raise
    return prepared_inputs, prepared, cached_frames


def rekey_cached_query(
    inputs: dict[str, Any],
    *,
    query_id: str,
    query_text: str,
    request_id: str,
    source_query_id: str = "q-free-form",
    score_cache_hit: bool = True,
) -> dict[str, Any]:
    """Create a distinct request over the same caller-owned fixture snapshots.

    A score-cache hit rekeys the fixture's precomputed scores. A miss leaves document and overlap scores under the
    original query ID while rekeying other request-scoped inputs, so Search must recompute them from the supplied
    term index. Neither mode models a production corpus or a cache-write lifecycle.
    """

    from pyspark.sql import functions as F

    normalized = " ".join(query_text.strip().lower().split())
    result = dict(inputs)
    result["queries"] = (
        inputs["queries"]
        .where(F.col("id") == source_query_id)
        .withColumn("id", F.lit(query_id))
        .withColumn("content", F.lit(query_text))
    )
    result["requests"] = (
        inputs["requests"]
        .where(F.col("query_id") == source_query_id)
        .withColumn("id", F.lit(request_id))
        .withColumn("query_id", F.lit(query_id))
        .withColumn("query", F.lit(normalized))
    )
    names = ["document_filter_targets", "document_filter_scores"]
    if score_cache_hit:
        names.extend(("document_scores", "document_overlap_scores"))
    for name in names:
        result[name] = inputs[name].where(F.col("query_id") == source_query_id).withColumn("query_id", F.lit(query_id))
    source_query = "aurora, beacon!" if source_query_id == "q-free-form" else "navigation"
    result["query_document_signals"] = (
        inputs["query_document_signals"].where(F.col("query") == source_query).withColumn("query", F.lit(normalized))
    )
    return result
