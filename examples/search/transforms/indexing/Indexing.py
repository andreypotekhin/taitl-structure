"""Search indexing composition."""

from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentIndexTerm,
    ParagraphIndexSummary,
    ParagraphIndexTerm,
    SectionIndexSummary,
    SectionIndexTerm,
    SentenceIndexSummary,
    SentenceIndexTerm,
)
from examples.search.schemas.text import Word
from examples.search.transforms.indexing.lexical.LexIndex import LexIndex
from structure import Transform, input, output


class Indexing(Transform):
    """Build all search indexes through explicit indexing stages."""

    words = input(Word)
    document_terms = output(DocumentIndexTerm)
    document_summary = output(DocumentIndexSummary)
    section_terms = output(SectionIndexTerm)
    section_summary = output(SectionIndexSummary)
    paragraph_terms = output(ParagraphIndexTerm)
    paragraph_summary = output(ParagraphIndexSummary)
    sentence_terms = output(SentenceIndexTerm)
    sentence_summary = output(SentenceIndexSummary)

    lexical = LexIndex(words=words)
