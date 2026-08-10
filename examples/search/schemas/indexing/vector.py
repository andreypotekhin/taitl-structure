"""Typed caller-owned vector-index artifacts for Search."""

from structure import Schema
from structure.plugin.pyspark import array, double, long, string


class VectorEmbedding(Schema):
    """One caller-produced embedding and its compatibility identity."""

    vector = array(double(), contains_null=False, nullable=False)
    model_id = string(nullable=False)
    dimension = long(nullable=False)
    content_revision = string(nullable=False)
    experiment_id = string(nullable=False)


class DocumentVectorEmbedding(VectorEmbedding):
    document_id = string(nullable=False)


class ParagraphVectorEmbedding(VectorEmbedding):
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)


class DocumentVectorQuery(VectorEmbedding):
    query_id = string(nullable=False)
    document_id = string(nullable=False)


class ParagraphVectorQuery(VectorEmbedding):
    query_id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)


class VectorIndexPolicy(Schema):
    """One caller-supplied identity and exact-retrieval policy row."""

    model_id = string(nullable=False)
    dimension = long(nullable=False)
    content_revision = string(nullable=False)
    experiment_id = string(nullable=False)
    maximum_candidates = long(nullable=False)
    rrf_k = long(nullable=False)


class DocumentVectorIndex(DocumentVectorEmbedding):
    """Validated document embeddings retained as the exact index artifact."""


class ParagraphVectorIndex(ParagraphVectorEmbedding):
    """Validated paragraph embeddings retained as the exact index artifact."""


class DocumentVectorIndexSummary(Schema):
    model_id = string(nullable=False)
    dimension = long(nullable=False)
    content_revision = string(nullable=False)
    experiment_id = string(nullable=False)
    target_count = long(nullable=False)


class ParagraphVectorIndexSummary(DocumentVectorIndexSummary):
    pass


class DocumentVectorScore(Schema):
    query_id = string(nullable=False)
    query_document_id = string(nullable=False)
    document_id = string(nullable=False)
    cosine_similarity = double(nullable=False)
    model_id = string(nullable=False)
    dimension = long(nullable=False)
    content_revision = string(nullable=False)
    experiment_id = string(nullable=False)


class DocumentVectorCandidate(DocumentVectorScore):
    rank = long(nullable=False)


class ParagraphVectorScore(Schema):
    query_id = string(nullable=False)
    query_document_id = string(nullable=False)
    query_section_id = string(nullable=False)
    query_paragraph_id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    cosine_similarity = double(nullable=False)
    model_id = string(nullable=False)
    dimension = long(nullable=False)
    content_revision = string(nullable=False)
    experiment_id = string(nullable=False)


class ParagraphVectorCandidate(ParagraphVectorScore):
    rank = long(nullable=False)
