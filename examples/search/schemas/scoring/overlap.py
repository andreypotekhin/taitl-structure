"""Overlap scoring schemas."""

from examples.search.schemas.search import (
    DocumentSearchTarget,
    ParagraphSearchTarget,
    SectionSearchTarget,
    SentenceSearchTarget,
)
from structure.plugin.pyspark import double


class DocumentOverlapScore(DocumentSearchTarget):
    score_overlap = double(nullable=False)


class SectionOverlapScore(SectionSearchTarget):
    score_overlap = double(nullable=False)


class ParagraphOverlapScore(ParagraphSearchTarget):
    score_overlap = double(nullable=False)


class SentenceOverlapScore(SentenceSearchTarget):
    score_overlap = double(nullable=False)
