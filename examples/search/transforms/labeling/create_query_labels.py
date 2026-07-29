"""Create caller-configured multilingual intent labels for search queries."""

from examples.search.schemas.label import (
    Intent,
    IntentPattern,
    LabelMapEntry,
    QueryIntentLabel,
    QueryLabelAssignmentEntries,
    QueryLabelAssignments,
)
from examples.search.schemas.search import SearchQuery
from structure import Transform, input, lane, output, raw, step
from structure.plugin.pyspark import (
    coalesce,
    collect_list,
    cross_join,
    group_by,
    lower,
    map_from_entries,
    require_all,
    require_reference,
    require_unique,
    trim,
    types,
)


class CreateQueryLabels(Transform):
    """Create binary English intent labels from one-pattern mappings."""

    fallback_language = "en_US"

    queries = input(SearchQuery)
    intents = input(Intent)
    patterns = input(IntentPattern)
    valid_intents = lane(Intent)
    valid_patterns = lane(IntentPattern)
    query_intents = lane(QueryIntentLabel)
    entries = lane(QueryLabelAssignmentEntries)
    labels = output(QueryLabelAssignments)

    @step(input=intents, output=valid_intents)
    def validate_intents(self, intent: Intent) -> Intent:
        require_unique(intent.id)
        require_unique(intent.name)
        require_all(trim(intent.name) != "")
        return Intent.project(intent)

    @step(input=[patterns, valid_intents], output=valid_patterns)
    def validate_patterns(self, pattern: IntentPattern, intent: Intent) -> IntentPattern:
        require_unique(pattern.intent_id, pattern.language, pattern.pattern)
        require_all((trim(pattern.language) != "") & (trim(pattern.pattern) != ""))
        require_reference(pattern.intent_id, intent, reference_key=intent.id, nulls="reject")
        return IntentPattern.project(pattern)

    @step(input=[queries, valid_intents], output=query_intents)
    def create_query_intents(self, query: SearchQuery, intent: Intent) -> QueryIntentLabel:
        cross_join(intent, allow_cartesian=True)
        return QueryIntentLabel(
            query_id=query.id,
            content=query.content,
            language=lower(coalesce(query.language, self.fallback_language)),
            intent_id=intent.id,
            name=intent.name,
            value=0,
        )

    @raw(inout=[lane(query_intents), lane(valid_patterns)] | lane(query_intents), target="pyspark")
    def match_patterns(self, *, query_intents, valid_patterns, spark, ctx):
        """Evaluate row-supplied regular expressions, unsupported by typed ``rlike``."""
        from pyspark.sql import functions as functions

        pairs = query_intents.alias("pair")
        patterns = valid_patterns.alias("pattern")
        matches = pairs.join(
            patterns,
            (functions.col("pair.intent_id") == functions.col("pattern.intent_id"))
            & (functions.col("pair.language") == functions.lower(functions.col("pattern.language"))),
            "left",
        ).withColumn(
            "value",
            functions.when(
                functions.expr("lower(trim(pair.content)) RLIKE pattern.pattern"), functions.lit(1)
            ).otherwise(functions.lit(0)),
        )
        return matches.groupBy("pair.query_id", "pair.content", "pair.language", "pair.intent_id", "pair.name").agg(
            functions.max("value").cast("long").alias("value")
        )

    @step(input=query_intents, output=entries)
    def collect_labels(self, label: QueryIntentLabel) -> QueryLabelAssignmentEntries:
        group_by(query_id=label.query_id)
        entries = collect_list(
            LabelMapEntry(key=label.name, value=label.value),
            element_type=types.struct(LabelMapEntry),
        )
        return QueryLabelAssignmentEntries(query_id=label.query_id, entries=entries)

    @step(input=entries, output=labels)
    def create_labels(self, entry: QueryLabelAssignmentEntries) -> QueryLabelAssignments:
        return QueryLabelAssignments(query_id=entry.query_id, labels=map_from_entries(entry.entries))
