"""Composed query labeling pipeline."""

from examples.search.schemas.label import Intent, IntentPattern, QueryLabel
from examples.search.schemas.search import SearchQuery
from examples.search.transforms.labeling.CreateQueryLabels import CreateQueryLabels
from examples.search.transforms.labeling.MergeQueryLabels import MergeQueryLabels
from structure import Transform, input, output


class Labeling(Transform):
    """Create intent labels and merge them with caller-supplied query labels."""

    queries = input(SearchQuery)
    query_labels = input(QueryLabel)
    intents = input(Intent)
    patterns = input(IntentPattern)

    created = CreateQueryLabels(
        queries=queries,
        intents=intents,
        patterns=patterns,
    )

    merged = MergeQueryLabels(
        queries=queries,
        query_labels=query_labels,
        created_labels=created.labels
    )

    labeled_queries = output(SearchQuery, merged.labeled_queries)
