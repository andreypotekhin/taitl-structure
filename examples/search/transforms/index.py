"""Public reusable-index transform entry point."""

from examples.search.transforms.indexing.Index import Index
from examples.search.transforms.score import ScoreAll


class CreateIndex(Index):
    """Build reusable document, section, paragraph, and sentence indexes."""


class EnrichWithScores(ScoreAll):
    """Attach reusable-index search scores to matching hierarchy rows."""

    overlap = ScoreAll.overlap
    bm25 = ScoreAll.bm25
    selected = ScoreAll.selected
