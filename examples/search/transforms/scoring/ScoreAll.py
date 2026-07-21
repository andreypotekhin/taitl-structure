"""Combined reusable-index scoring transform."""

from examples.search.transforms.scoring.ScoreBm25 import ScoreBm25


class ScoreAll(ScoreBm25):
    """Calculate overlap and BM25 scores against a prebuilt corpus index."""
