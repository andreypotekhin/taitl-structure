"""Composed query labeling pipeline."""

from examples.search.schemas.label import Intent, IntentPattern, QueryLabel
from examples.search.schemas.search import SearchQuery
from examples.search.transforms.labeling.create_query_labels import CreateQueryLabels
from examples.search.transforms.labeling.merge_query_labels import MergeQueryLabels
from structure import Transform, input, output, stage


class LabelQueries(Transform):
    """Create intent labels and merge them with caller-supplied query labels."""

    queries = input(SearchQuery)
    query_labels = input(QueryLabel)
    intents = input(Intent)
    patterns = input(IntentPattern)

    created = stage(
        CreateQueryLabels(queries=queries, intents=intents, patterns=patterns),
    )
    merged = stage(MergeQueryLabels(queries=queries, query_labels=query_labels, created_labels=created.labels))
    labeled_queries = output(SearchQuery).from_(merged.labeled_queries)
