"""Typed artifacts for indexed lexical similarity."""

from examples.texts.schemas.text import Document, Paragraph, Section, Sentence
from structure import Schema
from structure.plugin.pyspark import *


class SimilarityPolicy(Schema):
    """One caller-supplied policy row; null retains terms at every frequency."""

    max_document_frequency_ratio = double(nullable=True)


class SimilarityDocumentQuery(Document):
    """One caller-supplied document whose corpus neighbours are requested."""


class SimilaritySectionQuery(Section):
    """One caller-supplied section whose corpus neighbours are requested."""


class SimilarityParagraphQuery(Paragraph):
    """One caller-supplied paragraph whose corpus neighbours are requested."""


class SimilaritySentenceQuery(Sentence):
    """One caller-supplied sentence whose corpus neighbours are requested."""


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
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)


class IndexedSimilarDocument(Document):
    """A corpus document ranked for one query document by directed BM25."""

    rank = long(nullable=False)


class IndexedSimilarSection(Section):
    """A corpus section ranked for one query section by directed BM25."""

    rank = long(nullable=False)


class IndexedSimilarParagraph(Paragraph):
    """A corpus paragraph ranked for one query paragraph by directed BM25."""

    rank = long(nullable=False)


class IndexedSimilarSentence(Sentence):
    """A corpus sentence ranked for one query sentence by directed BM25."""

    rank = long(nullable=False)


class SectionSimilarity(Schema):
    left_document_id = string(nullable=False)
    left_section_id = string(nullable=False)
    right_document_id = string(nullable=False)
    right_section_id = string(nullable=False)
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
    score_overlap = double(nullable=False)
    bm25_left_to_right = double(nullable=False)
    bm25_right_to_left = double(nullable=False)
    bm25_mean = double(nullable=False)
