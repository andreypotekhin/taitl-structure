"""Intermediate schemas for reducing directed similarity scores."""

from structure import Schema
from structure.plugin.pyspark import double, string


class DocumentSimilarityCandidate(Schema):
    left_document_id = string(nullable=False)
    right_document_id = string(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)


class DocumentSimilarityPair(DocumentSimilarityCandidate):
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)


class SectionSimilarityCandidate(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)


class SectionSimilarityPair(SectionSimilarityCandidate):
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)


class ParagraphSimilarityCandidate(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    left_paragraph_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
    right_paragraph_id = string(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)


class ParagraphSimilarityPair(ParagraphSimilarityCandidate):
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)


class SentenceSimilarityCandidate(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    left_paragraph_id = string(nullable=False)
    left_sentence_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
    right_paragraph_id = string(nullable=False)
    right_sentence_id = string(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)


class SentenceSimilarityPair(SentenceSimilarityCandidate):
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)
