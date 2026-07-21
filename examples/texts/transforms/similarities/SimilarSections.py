"""Public section-similarity ranking transform."""

from examples.texts.transforms.similarities.SimilarityTargets import SimilarSections as SimilarSectionsBase


class SimilarSections(SimilarSectionsBase):
    """Return the top fixed number of corpus sections similar to one query section."""
