from structure import Schema
from structure.plugin.pyspark import *


class SearchQuery(Schema):
    """One caller-supplied full-text query."""

    id = string(nullable=False)
    content = string(nullable=False)


class DocumentSearchTarget(Schema):
    query_id = string(nullable=False)
    document_id = string(nullable=False)


class SectionSearchTarget(DocumentSearchTarget):
    section_id = string(nullable=False)


class ParagraphSearchTarget(SectionSearchTarget):
    paragraph_id = string(nullable=False)


class SentenceSearchTarget(ParagraphSearchTarget):
    sentence_id = string(nullable=False)


class DocumentOverlapScore(DocumentSearchTarget):
    score_overlap = double(nullable=False)


class SectionOverlapScore(SectionSearchTarget):
    score_overlap = double(nullable=False)


class ParagraphOverlapScore(ParagraphSearchTarget):
    score_overlap = double(nullable=False)


class SentenceOverlapScore(SentenceSearchTarget):
    score_overlap = double(nullable=False)


class DocumentBm25Score(DocumentSearchTarget):
    score_bm25 = double(nullable=False)


class SectionBm25Score(SectionSearchTarget):
    score_bm25 = double(nullable=False)


class ParagraphBm25Score(ParagraphSearchTarget):
    score_bm25 = double(nullable=False)


class SentenceBm25Score(SentenceSearchTarget):
    score_bm25 = double(nullable=False)
