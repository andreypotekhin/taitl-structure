"""Public paragraph-similarity ranking transform."""

from examples.texts.transforms.similarities.SimilarityTargets import SimilarParagraphs as SimilarParagraphsBase


class SimilarParagraphs(SimilarParagraphsBase):
    """Return the top fixed number of corpus paragraphs similar to one query paragraph."""
