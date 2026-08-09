"""Search indexing composition."""

from examples.search.schemas.fields import *
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentTerm,
    ParagraphIndexSummary,
    ParagraphTerm,
    SectionIndexSummary,
    SectionTerm,
    SentenceIndexSummary,
    SentenceTerm,
)
from examples.search.schemas.text import Document, Sentence
from examples.search.transforms.indexing.fields import FieldIndex
from examples.search.transforms.indexing.lexical.LexIndex import LexIndex
from structure import Transform, input, output


class Indexing(Transform):
    """Build all search indexes through explicit indexing stages."""

    documents = input(Document)
    sentences = input(Sentence)
    document_fields = input(DocumentField)
    field_profiles = input(FieldProfile)
    analyzer_policies = input(AnalyzerPolicy)
    document_terms = output(DocumentTerm)
    document_summary = output(DocumentIndexSummary)
    section_terms = output(SectionTerm)
    section_summary = output(SectionIndexSummary)
    paragraph_terms = output(ParagraphTerm)
    paragraph_summary = output(ParagraphIndexSummary)
    sentence_terms = output(SentenceTerm)
    sentence_summary = output(SentenceIndexSummary)
    lexical = LexIndex(documents=documents, sentences=sentences)
    fields = FieldIndex(
        document_fields=document_fields,
        field_profiles=field_profiles,
        analyzer_policies=analyzer_policies,
    )
    field_terms = output(FieldTerm, fields.terms)
