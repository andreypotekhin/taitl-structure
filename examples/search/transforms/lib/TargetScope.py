"""Stable identity for a bounded document-target universe."""

from structure.plugin.pyspark import concat_ws, sha2, types


def target_scope_id(query_id: object, scored_at: object, maximum_candidates: int) -> object:
    """Identify the filter snapshot and bound that produced a target universe."""

    return sha2(
        concat_ws(
            "\x1f",
            "document-filter-targets-v1",
            query_id,
            scored_at.cast(types.string()),  # type: ignore[attr-defined]
            str(maximum_candidates),
        ),
        bits=256,
    )
