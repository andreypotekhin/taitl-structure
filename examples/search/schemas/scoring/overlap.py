"""Overlap scoring schemas."""

from examples.search.schemas.search import (
    DocumentSearchTarget,
    ParagraphSearchTarget,
    SectionSearchTarget,
    SentenceSearchTarget,
)
from structure.plugin.pyspark import double, timestamp


class DocumentOverlapScore(DocumentSearchTarget):
    scored_at = timestamp(nullable=False)
    score_overlap = double(nullable=False)


class SectionOverlapScore(SectionSearchTarget):
    scored_at = timestamp(nullable=False)
    score_overlap = double(nullable=False)


class ParagraphOverlapScore(ParagraphSearchTarget):
    scored_at = timestamp(nullable=False)
    score_overlap = double(nullable=False)


class SentenceOverlapScore(SentenceSearchTarget):
    scored_at = timestamp(nullable=False)
    score_overlap = double(nullable=False)
