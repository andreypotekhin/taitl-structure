"""Shared Reciprocal Rank Fusion expressions."""

from structure.plugin.pyspark import coalesce, when


class Rrf:
    """Compiler-visible mappings for equal-weight Reciprocal Rank Fusion."""

    @staticmethod
    def score(lexical_rank, vector_rank, rrf_k):
        """Return the sum of contributions from the ranks that are present."""
        lexical = when(lexical_rank.is_not_null(), 1.0 / (rrf_k + lexical_rank)).otherwise(0.0)
        vector = when(vector_rank.is_not_null(), 1.0 / (rrf_k + vector_rank)).otherwise(0.0)
        return coalesce(lexical, 0.0) + coalesce(vector, 0.0)
