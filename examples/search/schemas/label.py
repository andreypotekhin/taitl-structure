"""Caller-supplied query labels."""

from structure import Schema
from structure.plugin.pyspark import *


class Label(Schema):
    """One named integral query label."""

    name = string(nullable=False)
    value = long(nullable=False)


class QueryLabel(Schema):
    """One timestamped label assignment for a caller-supplied query."""

    query_id = string(nullable=False)
    label = struct(Label, nullable=False)
    assigned_at = timestamp(nullable=False)


class Intent(Schema):
    """One canonical intent and its English evaluation-label name."""

    id = string(nullable=False)
    name = string(nullable=False)


class IntentPattern(Schema):
    """One language-specific regular-expression pattern for an intent."""

    intent_id = string(nullable=False)
    language = string(nullable=False)
    pattern = string(nullable=False)


class QueryIntentLabel(Schema):
    """One query-intent pair awaiting pattern evaluation."""

    query_id = string(nullable=False)
    content = string(nullable=False)
    language = string(nullable=False)
    intent_id = string(nullable=False)
    name = string(nullable=False)
    value = long(nullable=False)


class LabelMapEntry(Schema):
    """Internal map entry used while materializing query labels."""

    key = string(nullable=False)
    value = long(nullable=False)


class QueryLabelAssignments(Schema):
    """The latest label assignments collected for one query."""

    query_id = string(nullable=False)
    labels = map(string(), long(), value_contains_null=False, nullable=False)


class QueryLabelAssignmentEntries(Schema):
    """Collected latest label entries for one query."""

    query_id = string(nullable=False)
    entries = array(struct(LabelMapEntry), contains_null=False, nullable=False)
