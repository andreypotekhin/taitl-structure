"""Materialize latest caller-provided labels on search queries."""

from examples.search.schemas.label import LabelMapEntry, QueryLabel, QueryLabelAssignmentEntries, QueryLabelAssignments
from examples.search.schemas.search import SearchQuery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    array_contains,
    coalesce,
    collect_list,
    dedupe_latest_by,
    drop_duplicates,
    element_at,
    group_by,
    left_join,
    map_concat,
    map_filter,
    map_from_entries,
    map_keys,
    types,
    when,
)


class MergeQueryLabels(Transform):
    """Overlay each query's latest timestamped labels onto its generic label map."""

    queries = input(SearchQuery)
    query_labels = input(QueryLabel)
    latest_labels = lane(QueryLabel)
    entries = lane(QueryLabelAssignmentEntries)
    assignments = lane(QueryLabelAssignments)
    labeled_queries = output(SearchQuery)

    @step(input=query_labels, output=latest_labels)
    def select_latest(self, label: QueryLabel) -> QueryLabel:
        drop_duplicates(label.query_id, label.label.name, label.label.value, label.assigned_at)
        dedupe_latest_by(
            label.assigned_at,
            partition_by=(label.query_id, label.label.name),
            ties="error",
        )
        return QueryLabel.project(label)

    @step(input=latest_labels, output=entries)
    def collect_assignments(self, label: QueryLabel) -> QueryLabelAssignmentEntries:
        group_by(query_id=label.query_id)
        entries = collect_list(
            LabelMapEntry(key=label.label.name, value=label.label.value),
            element_type=types.struct(LabelMapEntry),
        )
        return QueryLabelAssignmentEntries(query_id=label.query_id, entries=entries)

    @step(input=entries, output=assignments)
    def create_assignments(self, entry: QueryLabelAssignmentEntries) -> QueryLabelAssignments:
        return QueryLabelAssignments(query_id=entry.query_id, labels=map_from_entries(entry.entries))

    @step(input=[queries, assignments], output=labeled_queries)
    def merge(self, query: SearchQuery, assignment: QueryLabelAssignments) -> SearchQuery:
        left_join(on=assignment.query_id == query.id)
        retained = map_filter(
            query.labels,
            lambda key, value: ~array_contains(map_keys(assignment.labels), key),
        )
        updated = when(assignment.query_id.is_not_null(), map_concat(retained, assignment.labels)).otherwise(
            query.labels
        )
        labels = coalesce(updated, query.labels)
        return SearchQuery(
            id=query.id,
            content=query.content,
            labels=labels,
            is_question=coalesce(element_at(labels, "is_question"), 0) == 1,
            is_time_sensitive=coalesce(element_at(labels, "is_time_sensitive"), 0) == 1,
        )
