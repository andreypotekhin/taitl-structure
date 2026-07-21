"""Combined document-corpus search scoring transform."""

from examples.texts.transforms.search.ScoreBm25 import ScoreBm25
from examples.texts.transforms.search.ScoreOverlap import ScoreOverlap


class ScoreCorpus(ScoreOverlap, ScoreBm25):
    """Score one corpus with both overlap and BM25 algorithms."""
