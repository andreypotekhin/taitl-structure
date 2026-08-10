"""Public production-score selection boundary."""

from examples.search.transforms.scoring.Scoring import Scoring


class EnrichWithScores(Scoring):
    """Attach reusable-index search scores to matching hierarchy rows."""

    overlap = Scoring.overlap
    bm25 = Scoring.bm25
    selected = Scoring.selected
