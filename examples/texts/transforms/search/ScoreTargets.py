"""Shared search-transform inputs."""

from examples.texts.schemas.search import SearchQuery
from examples.texts.schemas.text import Word
from structure import Transform, input


class ScoreTargets(Transform):
    """Provide caller-supplied queries and extracted words to score transforms."""

    queries = input(SearchQuery)
    words = input(Word)
