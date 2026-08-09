"""Compact positional postings for searchable document metadata fields."""

from examples.search.algorithms.text import normalized_token
from examples.search.schemas.fields import *
from structure import *
from structure.plugin.pyspark import *


class FieldText(Schema):
    """Internal field token before expansion."""

    term = string(nullable=False)


class ExpandedFieldText(Schema):
    """Internal field token with its field-local position."""

    position = long(nullable=False)
    term = string(nullable=False)


class FieldIndex(Transform):
    """Build normalized metadata postings while leaving ``LexIndex`` untouched."""

    document_fields = input(DocumentField)
    field_profiles = input(FieldProfile)
    analyzer_policies = input(AnalyzerPolicy)
    terms = output(FieldTerm)

    @step(input=[document_fields, field_profiles, analyzer_policies], output=terms)
    def tokenize(self, field: DocumentField, profile: FieldProfile, policy: AnalyzerPolicy) -> FieldTerm:
        inner_join(profile, on=(profile.field_name == field.field_name) | (profile.field_name == "*"))
        inner_join(policy, on=policy.policy_id == profile.analyzer_policy)
        where(profile.searchable)
        tokens = when(
            profile.field_kind == "keyword",
            array(FieldText(term=field.field_value)),
        ).otherwise(
            arr_transform(split(field.field_value, pattern=r"\s+"), lambda value: FieldText(term=value))
        )
        expanded = posexplode_struct(tokens, as_=ExpandedFieldText, ordinal="position", scope="field_term")
        term = normalized_token(expanded.term)
        where(term != "")
        where((profile.field_kind == "keyword") | ~array_contains(policy.stop_words, term))
        return FieldTerm(
            document_id=field.document_id,
            field_name=field.field_name,
            term=term,
            position=expanded.position,
            analyzer_policy=profile.analyzer_policy,
            phrase_enabled=profile.phrase_enabled,
        )
