import ast
import importlib
import json
from pathlib import Path
from typing import Any

from structure import Schema, Transform, input, output, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import array_contains, cross_join, long, size, string, where
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step

ROOT = Path(__file__).resolve().parents[3]
INVENTORY = ROOT / "docs/dev/specifications/V6ExampleRawHookInventory.json"
COMPOSED_TRANSFORMS = {
    "search.rerank-documents.score_candidates": (
        "examples.search.transforms.searching.search_docs.SearchDocuments",
        "SearchDocuments",
    ),
    "search.resolve-cohort-bands.resolve_bands": (
        "examples.search.transforms.cohorts.ResolveCohortBands",
        "ResolveCohortBands",
    ),
}
RETIRED_TRANSFORMS = {
    "search.chunking.chunk": (
        "examples.search.transforms.chunking.DocumentChunking",
        "DocumentChunking",
    ),
    "security.vulnerability-posture.retain-reconciled-inventory": (
        "examples.security.transforms.posture",
        "SecurityPosture",
    ),
    "security.vulnerability-quality.reconcile-device-inventory": (
        "examples.security.transforms.quality",
        "SecurityInventoryQuality",
    ),
    "search.score-overlap.score_overlap": (
        "examples.search.transforms.scoring.ScoreOverlap",
        "ScoreOverlap",
    ),
    "search.score-bm25.score_bm25": (
        "examples.search.transforms.scoring.ScoreBm25",
        "ScoreBm25",
    ),
    "search.index.build": (
        "examples.search.transforms.index",
        "CreateIndex",
    ),
    "search.create-similarity-queries.build": (
        "examples.search.transforms.similarities.CreateSimilarityQueries",
        "CreateSimilarityQueries",
    ),
    "search.reduce-similarity-scores.reduce": (
        "examples.search.transforms.similarities.ReduceSimilarityScores",
        "ReduceSimilarityScores",
    ),
    "search.build-relevance-signals.expand_impressions": (
        "examples.search.transforms.relevance.BuildRelevanceSignals",
        "BuildRelevanceSignals",
    ),
    "search.build-relevance-signals.expand_clicks": (
        "examples.search.transforms.relevance.BuildRelevanceSignals",
        "BuildRelevanceSignals",
    ),
    "search.rerank-documents.score_candidates": (
        "examples.search.transforms.searching.search_docs.SearchDocuments",
        "SearchDocuments",
    ),
}


def test_each_v6_example_hook_compiles_to_its_declared_opaque_boundary() -> None:
    for entry in _entries():
        transform, source_transform = _transform(entry)
        traceability = Compiler.traceability.build()(
            Compiler.frontend.compile()(transform, materialize_schemas=False).lowered,
            source_transform=source_transform,
            transform_module="v6_migration_fixture",
        )
        hooks = {boundary.hook for boundary in traceability.opaque_boundaries}
        if entry["status"] == "retired":
            assert entry["method"] not in hooks
        else:
            assert entry["method"] in hooks


def test_security_reconciliation_is_typed_and_has_no_opaque_hook_boundary() -> None:
    posture, posture_traceability = _lowered("examples.security.transforms.posture", "SecurityPosture")
    quality, quality_traceability = _lowered("examples.security.transforms.quality", "SecurityInventoryQuality")

    assert posture_traceability.opaque_boundaries == ()
    assert quality_traceability.opaque_boundaries == ()
    assert {"array_contains", "array_exists"} <= _functions(_step(posture, "expose").filters[0])
    assignments = {assignment.field.name: assignment.expression for assignment in _step(quality, "prepare_inventory_reconciliation").projection}
    assert {"array_exists"} <= _functions(assignments["device_has_software"])
    assert {"array_contains", "array_exists"} <= _functions(assignments["is_reconciled"])


def test_search_cohort_band_matcher_prerequisites_are_typed() -> None:
    from examples.search.schemas.user import Band, User

    class Match(Schema):
        user_id = string(nullable=False)
        band_id = string(nullable=False)
        priority = long(nullable=False)
        parent_band_id = string(nullable=True)

    @transform
    class CohortBandMatcher(Transform):
        users = input(User)
        bands = input(Band)
        matches = output(Match)

        def match(self, user: User, band: Band) -> Match:
            cross_join(band, allow_cartesian=True)
            where(
                ((size(band.genders) == 0) | array_contains(band.genders, user.gender))
                & ((size(band.locales) == 0) | array_contains(band.locales, user.locale))
                & ((size(band.countries) == 0) | array_contains(band.countries, user.country))
                & ((size(band.geo_tags) == 0) | array_contains(band.geo_tags, user.geo_tag))
                & ((size(band.device_types) == 0) | array_contains(band.device_types, user.device_type))
                & ((size(band.time_zones) == 0) | array_contains(band.time_zones, user.time_zone))
                & (band.age_start.is_null() | (user.age >= band.age_start))
                & (band.age_end.is_null() | (user.age < band.age_end))
            )
            return Match(
                user_id=user.id,
                band_id=band.id,
                priority=band.priority,
                parent_band_id=band.parent_band_id,
            )

    plan, traceability = _lowered_transform(CohortBandMatcher)
    predicate = _step(plan, "match").filters[0]
    functions = _functions(predicate)
    rendered = render_pyspark_step(
        _step(plan, "match"),
        current="users",
        sources={"users": "users", "bands": "bands"},
    )

    assert {"collection_size", "array_contains"} <= functions
    assert traceability.opaque_boundaries == ()
    assert ".crossJoin(" in rendered
    assert "F.array_contains" in rendered
    assert "F.size" in rendered


def test_search_bm25_scoring_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered("examples.search.transforms.scoring.ScoreBm25", "ScoreBm25")
    scoring_steps = [
        _step(plan, "score_document_bm25"),
        _step(plan, "score_section_bm25"),
        _step(plan, "score_paragraph_bm25"),
        _step(plan, "score_sentence_bm25"),
    ]

    assert traceability.opaque_boundaries == ()
    assert [step.name for step in plan.steps[:3]] == [
        "expand_query_terms",
        "select_distinct_query_terms",
        "count_query_terms",
    ]
    for step in scoring_steps:
        joins = [operation.join for operation in step.operations if operation.join is not None]
        aggregate = step.aggregate

        assert [join.how.value for join in joins] == ["inner", "cross"]
        assert aggregate is not None
        assert aggregate.grouping == "group_by"
        assert aggregate.assignments[-1].function == "sum"
        assert {"log"} <= _functions(aggregate.assignments[-1].expression)


def test_search_similarity_query_construction_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered(
        "examples.search.transforms.similarities.CreateSimilarityQueries",
        "CreateSimilarityQueries",
    )
    build_steps = [
        _step(plan, "build_document_queries"),
        _step(plan, "build_section_queries"),
        _step(plan, "build_paragraph_queries"),
        _step(plan, "build_sentence_queries"),
    ]
    merge = _step(plan, "merge_queries")

    assert traceability.opaque_boundaries == ()
    assert [_step(plan, "validate_policy").operations[0].kind] == ["require_all"]
    for step in build_steps:
        assert [operation.kind for operation in step.operations] == [
            "exactly_one",
            "join",
            "join",
            "filter",
            "aggregate",
        ]
        assert step.aggregate is not None
        assert step.aggregate.assignments[-1].function == "collect_list"
        assert step.aggregate.assignments[-1].order_by is not None
    assert [operation.kind for operation in merge.operations] == ["union_all", "union_all", "union_all"]


def test_search_index_build_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered("examples.search.transforms.index", "CreateIndex")

    assert traceability.opaque_boundaries == ()
    for grain in ("document", "section", "paragraph", "sentence"):
        count_terms = _step(plan, f"lexical.count_{grain}_terms")
        summarize_targets = _step(plan, f"lexical.summarize_{grain}s")
        count_frequencies = _step(plan, f"lexical.count_{grain}_frequencies")
        build_terms = _step(plan, f"lexical.build_{grain}_terms")
        summarize_index = _step(plan, f"lexical.summarize_{grain}_index")

        assert count_terms.aggregate is not None
        assert count_terms.aggregate.grouping == "group_by"
        assert count_terms.aggregate.assignments[-1].function == "count"
        assert summarize_targets.aggregate is not None
        assert summarize_targets.aggregate.grouping == "group_by"
        assert {
            assignment.function
            for assignment in summarize_targets.aggregate.assignments
            if assignment.function != "key"
        } == {
            "count",
            "count_distinct",
        }
        assert count_frequencies.aggregate is not None
        assert count_frequencies.aggregate.assignments[-1].function == "count"
        assert [operation.kind for operation in build_terms.operations] == ["join", "join"]
        assert summarize_index.aggregate is not None
        assert summarize_index.aggregate.grouping == "group_by"
        assert summarize_index.aggregate.keys == ()
        assert {assignment.function for assignment in summarize_index.aggregate.assignments} == {"count", "avg"}


def test_search_similarity_score_reduction_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered(
        "examples.search.transforms.similarities.ReduceSimilarityScores",
        "ReduceSimilarityScores",
    )

    assert traceability.opaque_boundaries == ()
    for grain in ("document", "section", "paragraph", "sentence"):
        assert [operation.kind for operation in _step(plan, f"canonical_{grain}_pairs").operations] == [
            "relation_alias",
            "filter",
            "join",
        ]
        assert [operation.kind for operation in _step(plan, f"merge_{grain}_pairs").operations] == ["union_all"]
        ranking = _step(plan, f"rank_{grain}_pairs")
        rank_expression = next(assignment.expression for assignment in ranking.projection if assignment.field.name == "rank")
        assert {"window_row_number"} <= _functions(rank_expression)
        assert len(_step(plan, f"publish_{grain}_pairs").filters) == 1


def test_search_relevance_context_fanout_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered(
        "examples.search.transforms.relevance.BuildRelevanceSignals",
        "BuildRelevanceSignals",
    )

    assert traceability.opaque_boundaries == ()
    for fact in ("impressions", "clicks"):
        assert [operation.kind for operation in _step(plan, f"fallback_{fact}").operations] == [
            "join",
            "join",
            "filter",
            "filter",
        ]
        assert [operation.kind for operation in _step(plan, f"band_{fact}").operations] == ["join", "filter"]
        assert [operation.kind for operation in _step(plan, f"merge_context_{fact}").operations] == [
            "union_all",
            "union_all",
        ]


def test_search_document_reranking_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered(
        "examples.search.transforms.searching.search_docs.SearchDocuments",
        "SearchDocuments",
    )

    assert traceability.opaque_boundaries == ()
    assert [operation.kind for operation in _step(plan, "reranked.select_fallback_options").operations] == [
        "filter",
        "filter",
        "join",
        "join",
    ]
    assert [operation.kind for operation in _step(plan, "reranked.merge_feedback_options").operations] == ["union_all"]
    assert [operation.kind for operation in _step(plan, "reranked.select_query_feedback").operations] == [
        "join",
        "select_first_qualified",
    ]
    assert [operation.kind for operation in _step(plan, "reranked.select_popularity_feedback").operations] == [
        "join",
        "select_first_qualified",
    ]
    assert [operation.kind for operation in _step(plan, "reranked.score_candidates").operations] == [
        "filter",
        "join",
        "join",
        "join",
    ]


def test_search_cohort_band_resolution_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered(
        "examples.search.transforms.cohorts.ResolveCohortBands",
        "ResolveCohortBands",
    )

    assert traceability.opaque_boundaries == ()
    assert [operation.kind for operation in _step(plan, "validate_bands").operations] == [
        "require_unique",
        "require_all",
        "require_parent_hierarchy",
    ]
    assert [operation.kind for operation in _step(plan, "match_bands").operations] == ["join", "filter"]
    assert [operation.kind for operation in _step(plan, "select_leaf_matches").operations] == [
        "relation_alias",
        "join",
    ]
    assert [operation.kind for operation in _step(plan, "expand_band_ancestors").operations] == [
        "hierarchy_closure"
    ]
    path = _step(plan, "build_user_band_paths")
    assert path.aggregate is not None
    assert path.aggregate.assignments[-1].function == "collect_list"
    assert path.aggregate.assignments[-1].order_by is not None
    assert [operation.kind for operation in _step(plan, "merge_user_band_catalog").operations] == [
        "union_all",
        "drop_duplicates",
    ]
    assert [operation.kind for operation in _step(plan, "merge_band_memberships").operations] == ["union_all"]
    assert [operation.kind for operation in _step(plan, "build_band_fallbacks").operations] == [
        "hierarchy_fallbacks"
    ]


def test_search_document_chunking_is_typed_and_has_no_opaque_hook_boundary() -> None:
    plan, traceability = _lowered("examples.search.transforms.chunking.DocumentChunking", "DocumentChunking")

    assert traceability.opaque_boundaries == ()
    assert [operation.kind for operation in _step(plan, "mark_lines").operations] == ["posexplode_struct"]
    line_assignments = {assignment.field.name: assignment.expression for assignment in _step(plan, "mark_lines").projection}
    assert {"window_sum"} <= _functions(line_assignments["section_ordinal"])
    assert {"window_sum"} <= _functions(line_assignments["paragraph_group"])

    paragraph_collect = _step(plan, "collect_paragraph_lines")
    assert paragraph_collect.aggregate is not None
    assert paragraph_collect.aggregate.assignments[-1].function == "collect_list"
    assert paragraph_collect.aggregate.assignments[-1].order_by is not None
    assert [operation.kind for operation in _step(plan, "select_section_keys").operations] == ["drop_duplicates"]
    assert [operation.kind for operation in _step(plan, "build_sections").operations] == ["join"]


def _entries() -> tuple[dict[str, Any], ...]:
    return tuple(json.loads(INVENTORY.read_text(encoding="utf-8"))["entries"])


def _transform(entry: dict[str, Any]) -> tuple[type, str]:
    declared = COMPOSED_TRANSFORMS.get(entry["id"]) or RETIRED_TRANSFORMS.get(entry["id"])
    if declared is not None:
        module_name, class_name = declared
        return getattr(importlib.import_module(module_name), class_name), f"{module_name}.{class_name}"
    path = Path(entry["path"])
    module_name = ".".join(path.with_suffix("").parts)
    class_name = _owner(path, entry["method"])
    return getattr(importlib.import_module(module_name), class_name), f"{module_name}.{class_name}"


def _owner(path: Path, method: str) -> str:
    for node in ast.parse((ROOT / path).read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and any(
            isinstance(member, ast.FunctionDef) and member.name == method for member in node.body
        ):
            return node.name
    raise AssertionError(f"No owner for {path}:{method}")


def _lowered(module_name: str, class_name: str):
    transform = getattr(importlib.import_module(module_name), class_name)
    return _lowered_transform(transform, source_transform=f"{module_name}.{class_name}")


def _lowered_transform(transform: type, *, source_transform: str | None = None):
    lowered = Compiler.frontend.compile()(transform, materialize_schemas=False).lowered
    traceability = Compiler.traceability.build()(
        lowered,
        source_transform=source_transform or transform.__name__,
        transform_module="v6_migration_fixture",
    )
    return lowered, traceability


def _step(lowered, name: str):
    return next(step for step in lowered.steps if step.name == name)


def _functions(expression) -> set[str]:
    return {
        str(item.data["function"])
        for item in _walk(expression)
        if item.data is not None and "function" in item.data
    }


def _walk(expression):
    yield expression
    for argument in expression.args:
        yield from _walk(argument)
