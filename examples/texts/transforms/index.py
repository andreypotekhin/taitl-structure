"""Public reusable-index transform entry point."""

from examples.texts.transforms.indexing.Index import Index
from examples.texts.transforms.scoring.AddScores import AddScores


class CreateIndex(Index):
    """Build reusable document, section, paragraph, and sentence indexes."""


class EnrichWithScores(AddScores):
    """Attach reusable-index search scores to matching hierarchy rows."""
