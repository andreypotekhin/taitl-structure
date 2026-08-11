"""Hybrid lexical/vector similarity schemas."""

from structure import Schema
from structure.plugin.pyspark import double, long, string


class DocumentFusedSimilarityCandidate(Schema):
    left_document_id = string(nullable=False)
    right_document_id = string(nullable=False)
    lexical_rank = long(nullable=True)
    vector_rank = long(nullable=True)
    score_overlap = double(nullable=True)
    bm25_left_to_right = double(nullable=True)
    bm25_right_to_left = double(nullable=True)
    bm25_mean = double(nullable=True)
    vector_similarity = double(nullable=True)
    vector_backend = string(nullable=True)
    vector_model_id = string(nullable=True)
    vector_dimension = long(nullable=True)
    vector_content_revision = string(nullable=True)
    rrf_score = double(nullable=False)
    rrf_k = long(nullable=False)
    experiment_id = string(nullable=False)


class DocumentFusedSimilarity(DocumentFusedSimilarityCandidate):
    rank = long(nullable=False)


class ParagraphFusedSimilarityCandidate(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    left_paragraph_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
    right_paragraph_id = string(nullable=False)
    lexical_rank = long(nullable=True)
    vector_rank = long(nullable=True)
    score_overlap = double(nullable=True)
    bm25_left_to_right = double(nullable=True)
    bm25_right_to_left = double(nullable=True)
    bm25_mean = double(nullable=True)
    vector_similarity = double(nullable=True)
    vector_backend = string(nullable=True)
    vector_model_id = string(nullable=True)
    vector_dimension = long(nullable=True)
    vector_content_revision = string(nullable=True)
    rrf_score = double(nullable=False)
    rrf_k = long(nullable=False)
    experiment_id = string(nullable=False)


class ParagraphFusedSimilarity(ParagraphFusedSimilarityCandidate):
    rank = long(nullable=False)
