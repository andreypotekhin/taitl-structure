"""BM25 scoring schemas."""

from examples.search.schemas.search import (
    DocumentSearchTarget,
    ParagraphSearchTarget,
    SectionSearchTarget,
    SentenceSearchTarget,
)
from structure.plugin.pyspark import double


class DocumentBm25Score(DocumentSearchTarget):
    score_bm25 = double(nullable=False)


class SectionBm25Score(SectionSearchTarget):
    score_bm25 = double(nullable=False)


class ParagraphBm25Score(ParagraphSearchTarget):
    score_bm25 = double(nullable=False)


class SentenceBm25Score(SentenceSearchTarget):
    score_bm25 = double(nullable=False)
