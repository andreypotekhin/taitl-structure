"""Typed artifacts for indexed lexical similarity."""

from examples.search.schemas.text import Document, Paragraph, Section, Sentence
from structure import Schema
from structure.plugin.pyspark import *


class SimilarityPolicy(Schema):
    """One caller-supplied policy row; null retains terms at every frequency."""

    max_document_frequency_ratio = double(nullable=True)


class SimilarityFusionPolicy(Schema):
    """One policy row for lexical/vector candidate fusion and presentation."""

    rrf_k = long(nullable=False)
    maximum_lexical_candidates = long(nullable=False)
    maximum_vector_candidates = long(nullable=False)
    maximum_results = long(nullable=False)
    experiment_id = string(nullable=False)


class SimilaritySearchQuery(Document):
    """One caller-supplied document whose corpus neighbours are requested."""


class SimilaritySectionQuery(Section):
    """One caller-supplied section whose corpus neighbours are requested."""

    search_query_id = string(nullable=True)
    score_overlap = double(nullable=True)
    score_bm25 = double(nullable=True)


class SimilarityParagraphQuery(Paragraph):
    """One caller-supplied paragraph whose corpus neighbours are requested."""

    search_query_id = string(nullable=True)
    score_overlap = double(nullable=True)
    score_bm25 = double(nullable=True)


class SimilaritySentenceQuery(Sentence):
    """One caller-supplied sentence whose corpus neighbours are requested."""

    search_query_id = string(nullable=True)
    score_overlap = double(nullable=True)
    score_bm25 = double(nullable=True)


class DocumentSimilarityQuery(Schema):
    query_id = string(nullable=False)
    document_id = string(nullable=False)


class SectionSimilarityQuery(DocumentSimilarityQuery):
    section_id = string(nullable=False)


class ParagraphSimilarityQuery(SectionSimilarityQuery):
    paragraph_id = string(nullable=False)


class SentenceSimilarityQuery(ParagraphSimilarityQuery):
    sentence_id = string(nullable=False)


class DocumentSimilarity(Schema):
    left_document_id = string(nullable=False)
    right_document_id = string(nullable=False)
    rank = long(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)


class IndexedSimilarDocument(Document):
    """A document result with inspectable lexical/vector fusion evidence."""

    lexical_rank = long(nullable=True)
    vector_rank = long(nullable=True)
    vector_similarity = double(nullable=True)
    rrf_k = long(nullable=False)
    rrf_score = double(nullable=False)
    vector_backend = string(nullable=True)
    vector_model_id = string(nullable=True)
    vector_dimension = long(nullable=True)
    vector_content_revision = string(nullable=True)
    experiment_id = string(nullable=False)
    rank = long(nullable=False)


class IndexedSimilarParagraph(Paragraph):
    """A paragraph result with inspectable lexical/vector fusion evidence."""

    lexical_rank = long(nullable=True)
    vector_rank = long(nullable=True)
    score_overlap = double(nullable=True)
    score_bm25 = double(nullable=True)
    vector_similarity = double(nullable=True)
    rrf_k = long(nullable=False)
    rrf_score = double(nullable=False)
    vector_backend = string(nullable=True)
    vector_model_id = string(nullable=True)
    vector_dimension = long(nullable=True)
    vector_content_revision = string(nullable=True)
    experiment_id = string(nullable=False)
    rank = long(nullable=False)


class SectionSimilarity(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
    rank = long(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)


class ParagraphSimilarity(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    left_paragraph_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
    right_paragraph_id = string(nullable=False)
    rank = long(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)


class SentenceSimilarity(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    left_paragraph_id = string(nullable=False)
    left_sentence_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
    right_paragraph_id = string(nullable=False)
    right_sentence_id = string(nullable=False)
    rank = long(nullable=False)
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)
