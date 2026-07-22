"""Public reusable-index transform entry point."""

from examples.search.transforms.indexing.Index import Index
from examples.search.transforms.score import AddScores


class CreateIndex(Index):
    """Build reusable document, section, paragraph, and sentence indexes."""


class EnrichWithScores(AddScores):
    """Attach reusable-index search scores to matching hierarchy rows."""
