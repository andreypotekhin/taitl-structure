"""Implicit-feedback document reranking."""

from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.search import DocumentSearchCandidate, DocumentSearchResult
from examples.search.schemas.user import BandFallback
from examples.search.transforms.searching.search_docs.RetrieveDocuments import RetrieveDocuments
from structure import Transform, input, lane, output, raw, step
from structure.plugin.pyspark import (
    row_number,
    rows_between,
    unbounded_following,
    unbounded_preceding,
    window,
    window_max,
)


class RerankDocuments(Transform):
    """Enrich lexical candidates with independently resolved feedback signals."""

    query_document_signals = input(QueryDocumentSignals)
    document_popularity = input(DocumentPopularity)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)
    scored_candidates = lane(DocumentSearchCandidate)
    normalized_candidates = lane(DocumentSearchCandidate)
    results = output(DocumentSearchResult)

    @step(input=lane(RetrieveDocuments.candidates), output=scored_candidates)
    def declare_scored_candidates(self, candidate: DocumentSearchCandidate) -> DocumentSearchCandidate:
        """Declare the feedback-enriched lane before its ordered raw selection."""

        return DocumentSearchCandidate.project(candidate)(
            score_feedback=0.0,
            score_rank=0.0,
            score_weight=0.0,
            feedback_weight=0.0,
        )

    @raw(
        input=[lane(RetrieveDocuments.candidates), input(query_document_signals), input(document_popularity), input(band_fallbacks), input(policy)],
        output=scored_candidates,
    )
    def score_candidates(self, *, candidates, query_document_signals, document_popularity, band_fallbacks, policy, scored_candidates, spark, ctx):
        """Choose exact, parent, then global feedback without blending fallback levels."""

        from pyspark.sql import Window
        from pyspark.sql import functions as F

        policy = policy.select(
            F.col("minimum_band_impressions").alias("_minimum_band_impressions"),
            F.col("score_weight").alias("_score_weight"),
            F.col("feedback_weight").alias("_feedback_weight"),
        )
        candidates = candidates.where(F.col("candidate_rank") <= RetrieveDocuments.maximum_candidates).withColumn(
            "_candidate_id", F.monotonically_increasing_id()
        ).crossJoin(policy)
        fallback = band_fallbacks.select(
            F.col("band_id").alias("_source_band_id"), "fallback_band_id", "ordinal"
        )
        scoped = candidates.alias("candidate").where(F.col("candidate.band_id").isNotNull()).join(
            fallback.alias("fallback"),
            F.col("candidate.band_id") == F.col("fallback._source_band_id"),
            "inner",
        ).select("candidate.*", F.col("fallback.fallback_band_id"), F.col("fallback.ordinal"))
        global_candidates = candidates.where(F.col("band_id").isNull()).withColumn(
            "fallback_band_id", F.lit(None).cast("string")
        ).withColumn("ordinal", F.lit(0).cast("long"))
        options = scoped.unionByName(global_candidates, allowMissingColumns=False)

        def select_signal(signals, query_specific, alias):
            condition = (
                F.col("option.document_id") == F.col("signal.document_id")
            ) & F.col("signal.band_id").eqNullSafe(F.col("option.fallback_band_id"))
            if query_specific:
                condition = condition & (F.col("option.query") == F.col("signal.query"))
            choices = options.alias("option").join(signals.alias("signal"), condition, "left").where(
                F.col("option.fallback_band_id").isNull()
                | (F.col("signal.impression_count") >= F.col("option._minimum_band_impressions"))
            )
            return choices.withColumn(
                "_choice", F.row_number().over(Window.partitionBy("option._candidate_id").orderBy("option.ordinal"))
            ).where(F.col("_choice") == 1).select(
                F.col("option._candidate_id"), F.col("signal.normalized_score").alias(alias)
            )

        query_feedback = select_signal(query_document_signals, True, "query_feedback")
        popularity_feedback = select_signal(document_popularity, False, "popularity_feedback")
        scored = candidates.alias("candidate").join(query_feedback, "_candidate_id", "left").join(
            popularity_feedback, "_candidate_id", "left"
        )
        return scored.select(
            *[
                (
                    (0.8 * F.coalesce(F.col("query_feedback"), F.lit(0.0)) + 0.2 * F.coalesce(F.col("popularity_feedback"), F.lit(0.0))).alias(name)
                    if name == "score_feedback"
                    else F.lit(0.0).alias(name)
                    if name == "score_rank"
                    else F.col("candidate._score_weight").alias(name)
                    if name == "score_weight"
                    else F.col("candidate._feedback_weight").alias(name)
                    if name == "feedback_weight"
                    else F.col(f"candidate.{name}").alias(name)
                )
                for name in scored_candidates.columns
            ]
        )

    @step(input=scored_candidates, output=normalized_candidates)
    def normalize_score(self, candidate: DocumentSearchCandidate) -> DocumentSearchCandidate:
        """Normalize lexical scores inside one query, band, and experiment ranking."""

        maximum = window_max(
            candidate.score,
            over=window(
                partition_by=(candidate.search_query_id, candidate.band_id, candidate.experiment_id),
                order_by=candidate.document_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        return DocumentSearchCandidate.project(candidate)(
            score_rank=candidate.score_weight * candidate.score / maximum
            + candidate.feedback_weight * candidate.score_feedback,
        )

    @step(input=normalized_candidates, output=results)
    def rank_results(self, candidate: DocumentSearchCandidate) -> DocumentSearchResult:
        """Publish deterministic ranks for one query, band, and experiment."""

        return DocumentSearchResult.base(candidate)(
            rank=row_number(
                partition_by=(candidate.search_query_id, candidate.band_id, candidate.experiment_id),
                order_by=(candidate.score_rank.desc_nulls_last(), candidate.document_id.asc_nulls_first()),
            ),
        )
