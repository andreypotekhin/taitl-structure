"""Shared reusable-index scoring inputs."""

from examples.search.schemas.search import (
    DocumentIndexTerm,
    ParagraphIndexTerm,
    SearchQuery,
    SectionIndexTerm,
    SentenceIndexTerm,
)
from structure import Transform, input


class ScoreBase(Transform):
    """Accept one or more queries and four reusable target-grain indexes."""

    queries = input(SearchQuery)
    document_terms = input(DocumentIndexTerm)
    section_terms = input(SectionIndexTerm)
    paragraph_terms = input(ParagraphIndexTerm)
    sentence_terms = input(SentenceIndexTerm)
