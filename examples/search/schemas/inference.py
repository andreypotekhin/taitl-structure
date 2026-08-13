"""Inference configuration and observable result contracts."""

from structure import Schema
from structure.plugin.pyspark import array, double, long, string, timestamp


class InferencePolicy(Schema):
    """Provider/model identity and vector-shape policy for inference."""

    provider_id = string(nullable=False)
    model_id = string(nullable=False)
    model_version = string(nullable=False)
    content_revision = string(nullable=False)
    dimension = long(nullable=False)
    experiment_id = string(nullable=False)
    inferred_at = timestamp(nullable=False)


class QueryInferenceResult(Schema):
    """One adapter result before successful embeddings are published."""

    query_id = string(nullable=False)
    vector = array(double(), contains_null=False, nullable=True)
    status = string(nullable=False)
    error_code = string(nullable=True)
    diagnostic = string(nullable=True)


class DocumentInferenceResult(Schema):
    """One adapter result before successful embeddings are published."""

    document_id = string(nullable=False)
    vector = array(double(), contains_null=False, nullable=True)
    status = string(nullable=False)
    error_code = string(nullable=True)
    diagnostic = string(nullable=True)


class QueryInferenceStatus(Schema):
    """Observable status for one query inference attempt."""

    query_id = string(nullable=False)
    provider_id = string(nullable=False)
    model_id = string(nullable=False)
    model_version = string(nullable=False)
    status = string(nullable=False)
    error_code = string(nullable=True)
    diagnostic = string(nullable=True)
    inferred_at = timestamp(nullable=False)


class DocumentInferenceStatus(Schema):
    """Observable status for one document inference attempt."""

    document_id = string(nullable=False)
    provider_id = string(nullable=False)
    model_id = string(nullable=False)
    model_version = string(nullable=False)
    status = string(nullable=False)
    error_code = string(nullable=True)
    diagnostic = string(nullable=True)
    inferred_at = timestamp(nullable=False)
