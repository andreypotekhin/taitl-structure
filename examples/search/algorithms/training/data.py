"""Adapters and validation for persisted Search training snapshots."""

from collections.abc import Mapping, Sequence
from numbers import Real

from examples.search.algorithms.training.rankers import TrainingExample


def training_examples(rows: Sequence[Mapping[str, object]]) -> tuple[TrainingExample, ...]:
    """Convert persisted ``DocumentTrainingData`` rows into ranker input.

    A snapshot must contain only one judgment for each query/document pair;
    conflicting duplicates make offline evaluation ambiguous and fail early.
    """
    seen: set[tuple[object, object]] = set()
    examples = []
    for row in rows:
        key = (row["search_query_id"], row["document_id"])
        if key in seen:
            raise ValueError(f"Training snapshot duplicates judgment for query/document {key[0]!r}/{key[1]!r}.")
        seen.add(key)
        grade = row["relevance_grade"]
        if not isinstance(grade, (int, float)) or grade not in (0, 1, 2, 3):
            raise ValueError("Training snapshot relevance_grade must be one of 0, 1, 2, or 3.")
        examples.append(
            TrainingExample(
                str(key[0]),
                str(key[1]),
                float(grade),
                {
                    "lexical_score": _number(row["lexical_score"], "lexical_score"),
                    "query_token_count": _number(row["query_token_count"], "query_token_count"),
                    "query_distinct_token_count": _number(
                        row["query_distinct_token_count"], "query_distinct_token_count"
                    ),
                    "document_content_length": _number(row["document_content_length"], "document_content_length"),
                    "document_url_is_https": float(bool(row["document_url_is_https"])),
                },
            )
        )
    return tuple(examples)


def _number(value: object, name: str) -> float:
    if not isinstance(value, Real):
        raise ValueError(f"Training snapshot {name} must be numeric.")
    return float(value)
