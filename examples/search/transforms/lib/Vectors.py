"""Shared vector expressions for Search transforms."""

from structure.plugin.pyspark import arr_aggregate, arr_filter, arr_zip_with, isnan, size, sqrt

_MAX_FINITE_DOUBLE = 1.7976931348623157e308


class Vectors:
    """Compiler-visible mappings for vector identity, validation, and similarity."""

    @staticmethod
    def valid_policy(policy):
        """Return the row-local validity predicate for an exact-vector policy."""
        return (
            (policy.model_id != "")
            & (policy.dimension > 0)
            & (policy.content_revision != "")
            & (policy.experiment_id != "")
            & (policy.maximum_candidates > 0)
            & (policy.rrf_k > 0)
        )

    @staticmethod
    def valid_embedding(embedding, policy):
        """Return the validity predicate for an indexed embedding."""
        return (
            (embedding.model_id == policy.model_id)
            & (embedding.dimension == policy.dimension)
            & (embedding.content_revision == policy.content_revision)
            & (embedding.experiment_id == policy.experiment_id)
            & Vectors.valid_vector(embedding.vector, embedding.dimension)
        )

    @staticmethod
    def valid_pair(query, index, policy):
        """Return the validity predicate for a query/index vector pair."""
        return (
            (query.model_id == policy.model_id)
            & (index.model_id == policy.model_id)
            & (query.dimension == policy.dimension)
            & (index.dimension == policy.dimension)
            & (query.content_revision == policy.content_revision)
            & (index.content_revision == policy.content_revision)
            & (query.experiment_id == policy.experiment_id)
            & (index.experiment_id == policy.experiment_id)
            & Vectors.valid_vector(query.vector, query.dimension)
            & Vectors.valid_vector(index.vector, index.dimension)
        )

    @staticmethod
    def valid_vector(vector, dimension):
        """Return the validity predicate for a non-empty finite non-zero vector."""
        finite_values = arr_filter(
            vector,
            lambda value: isnan(value) | (value > _MAX_FINITE_DOUBLE) | (value < -_MAX_FINITE_DOUBLE),
        )
        norm = sqrt(arr_aggregate(vector, 0.0, lambda total, value: total + value * value))
        return (dimension == size(vector)) & (size(vector) > 0) & (size(finite_values) == 0) & (norm > 0.0)

    @staticmethod
    def cosine(left, right):
        """Return the cosine similarity expression for two validated vectors."""
        products = arr_zip_with(left, right, lambda left_value, right_value: left_value * right_value)
        dot = arr_aggregate(products, 0.0, lambda total, value: total + value)
        left_norm = sqrt(arr_aggregate(left, 0.0, lambda total, value: total + value * value))
        right_norm = sqrt(arr_aggregate(right, 0.0, lambda total, value: total + value * value))
        return dot / (left_norm * right_norm)
