"""Public sentence-similarity ranking transform."""

from examples.texts.transforms.similarities.SimilarityTargets import SimilarSentences as SimilarSentencesBase


class SimilarSentences(SimilarSentencesBase):
    """Return the top fixed number of corpus sentences similar to one query sentence."""
