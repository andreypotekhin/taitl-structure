"""Pure-Python parsing helpers for the typed field-search relations."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedFieldSearch:
    """Reader-friendly parser result before relations are materialized."""

    query: dict[str, object]
    terms: tuple[dict[str, object], ...]

DEFAULT_METADATA_STOP_WORDS = frozenset(
    {"a", "an", "and", "at", "by", "for", "in", "is", "of", "on", "or", "the", "to", "with"}
)
_FIELD_PREFIX = re.compile(r"^(?P<field>[A-Za-z_][A-Za-z0-9_.-]*):(?P<value>.*)$")
_TOKEN = re.compile(r"\S+")


def parse_field_search_query(
    query_id: str,
    text: str,
    *,
    queryset: str = "default",
    requested_at=None,
    language: str | None = None,
    stop_words: frozenset[str] = DEFAULT_METADATA_STOP_WORDS,
) -> ParsedFieldSearch:
    """Parse field prefixes and produce normalized metadata terms.

    Metadata phrases use field-index positions. ``content:`` is deliberately kept as
    raw full-text text; callers pass it to the existing full-text query path.
    """

    tokens = _scan(text)
    if not tokens:
        raise ValueError("field search query must contain at least one term")
    clauses: list[tuple[str | None, str, bool]] = []
    operators: list[str] = []
    expect_clause = True
    for token in tokens:
        if not expect_clause:
            if token in {"and", "or"}:
                operators.append(token)
                expect_clause = True
                continue
            if token.lower() in {"and", "or"}:
                raise ValueError("field search query requires lowercase 'and' or 'or' between clauses")
            operators.append("and")
            expect_clause = True
        if expect_clause:
            if token in {"and", "or"}:
                raise ValueError("field search query cannot start with a boolean operator")
            match = _FIELD_PREFIX.match(token)
            if match:
                field_name = match.group("field").lower()
                value = match.group("value")
            else:
                field_name = None
                value = token
            quoted = value.startswith('"') and value.endswith('"') and len(value) >= 2
            if value.startswith('"') != value.endswith('"'):
                raise ValueError(f"unterminated phrase in field search query: {token!r}")
            clauses.append((field_name, value[1:-1] if quoted else value, quoted))
            expect_clause = False
    if expect_clause:
        raise ValueError("field search query cannot end with a boolean operator")
    operator = operators[0] if operators else "and"
    if any(value != operator for value in operators):
        raise ValueError("mixed boolean operators are not supported; use one lowercase operator")

    terms: list[dict[str, object]] = []
    content_values: list[str] = []
    clause_count = 0
    for clause_ordinal, (field_name, value, is_phrase) in enumerate(clauses):
        if field_name == "content":
            content_values.append(value)
            continue
        clause_count += 1
        term_ordinal = 0
        clause_terms = []
        for raw_term in _TOKEN.findall(value):
            normalized = _normalize(raw_term)
            if normalized in stop_words or not normalized:
                term_ordinal += 1
                continue
            clause_terms.append(normalized)
            terms.append(
                {
                    "query_id": query_id,
                    "clause_ordinal": clause_ordinal,
                    "term_ordinal": term_ordinal,
                    "field_name": field_name,
                    "term": normalized,
                    "term_count": 0,
                    "is_phrase": is_phrase,
                }
            )
            term_ordinal += 1
        if not clause_terms:
            raise ValueError(f"field clause {clause_ordinal} contains no searchable terms")
        for term in terms:
            if term["clause_ordinal"] == clause_ordinal:
                term["term_count"] = len(clause_terms)

    if content_values and operator == "or" and clause_count:
        raise ValueError("content: cannot participate in an 'or' field query")
    return ParsedFieldSearch(
        query={
            "id": query_id,
            "queryset": queryset,
            "query_text": text,
            "content": " ".join(content_values),
            "requested_at": requested_at,
            "language": language,
            "default_scope": "metadata",
            "operator": operator,
            "clause_count": clause_count,
            "requires_content": bool(content_values),
        },
        terms=tuple(terms),
    )


def _scan(text: str) -> list[str]:
    values: list[str] = []
    current: list[str] = []
    quoted = False
    for character in text.strip():
        if character == '"':
            quoted = not quoted
            current.append(character)
        elif character.isspace() and not quoted:
            if current:
                values.append("".join(current))
                current = []
        else:
            current.append(character)
    if quoted:
        raise ValueError("unterminated phrase in field search query")
    if current:
        values.append("".join(current))
    return values


def _normalize(value: str) -> str:
    return re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", value.strip()).lower()
