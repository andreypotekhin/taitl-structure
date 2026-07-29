"""Public reusable-index transform entry point."""

from examples.search.transforms.indexing.Indexing import Indexing
from examples.search.transforms.score import Scoring


class CreateIndex(Indexing):
    """Build reusable document, section, paragraph, and sentence indexes."""

    lexical = Indexing.lexical
    document_terms = Indexing.document_terms
    document_summary = Indexing.document_summary
    section_terms = Indexing.section_terms
    section_summary = Indexing.section_summary
    paragraph_terms = Indexing.paragraph_terms
    paragraph_summary = Indexing.paragraph_summary
    sentence_terms = Indexing.sentence_terms
    sentence_summary = Indexing.sentence_summary


class EnrichWithScores(Scoring):
    """Attach reusable-index search scores to matching hierarchy rows."""

    overlap = Scoring.overlap
    bm25 = Scoring.bm25
    selected = Scoring.selected
