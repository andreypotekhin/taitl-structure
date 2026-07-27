"""Intermediate schemas for building similarity self-queries."""

from examples.search.schemas.similarity import (
    DocumentSimilarityQuery,
    ParagraphSimilarityQuery,
    SectionSimilarityQuery,
    SentenceSimilarityQuery,
)
from structure.plugin.pyspark import array, string


class DocumentSimilarityQueryText(DocumentSimilarityQuery):
    content_tokens = array(string(), contains_null=False, nullable=False)


class SectionSimilarityQueryText(SectionSimilarityQuery):
    content_tokens = array(string(), contains_null=False, nullable=False)


class ParagraphSimilarityQueryText(ParagraphSimilarityQuery):
    content_tokens = array(string(), contains_null=False, nullable=False)


class SentenceSimilarityQueryText(SentenceSimilarityQuery):
    content_tokens = array(string(), contains_null=False, nullable=False)
